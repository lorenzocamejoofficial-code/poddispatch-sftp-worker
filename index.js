const { createClient } = require("@supabase/supabase-js");
const SftpClient = require("ssh2-sftp-client");
const ws = require("ws");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SFTP_PASSWORD = process.env.SFTP_PASSWORD;
const SFTP_OUTBOUND_DIR = process.env.SFTP_OUTBOUND_DIR || "/outbound";
const OUTBOUND_POLL_INTERVAL_MS = parseInt(process.env.OUTBOUND_POLL_INTERVAL_MS || "300000", 10);
const ACK_INGEST_URL = process.env.ACK_INGEST_URL || "";
const ACK_INGEST_SHARED_SECRET = process.env.ACK_INGEST_SHARED_SECRET || "";

console.log("ENV CHECK:");
console.log("  SUPABASE_URL:", SUPABASE_URL ? "SET (" + SUPABASE_URL.length + " chars)" : "MISSING");
console.log("  SUPABASE_SERVICE_ROLE_KEY:", SUPABASE_SERVICE_ROLE_KEY ? "SET (" + SUPABASE_SERVICE_ROLE_KEY.length + " chars)" : "MISSING");
console.log("  SFTP_PASSWORD:", SFTP_PASSWORD ? "SET (" + SFTP_PASSWORD.length + " chars)" : "MISSING");
console.log("  SFTP_OUTBOUND_DIR:", SFTP_OUTBOUND_DIR);
console.log("  OUTBOUND_POLL_INTERVAL_MS:", OUTBOUND_POLL_INTERVAL_MS);
console.log("  ACK_INGEST_URL:", ACK_INGEST_URL ? "SET" : "MISSING");
console.log("  ACK_INGEST_SHARED_SECRET:", ACK_INGEST_SHARED_SECRET ? "SET (" + ACK_INGEST_SHARED_SECRET.length + " chars)" : "MISSING");

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !SFTP_PASSWORD) {
  console.error("Missing required env vars - exiting");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
  realtime: { transport: ws },
});

const SFTP_CONFIG = {
  host: "ftp10.officeally.com",
  port: 22,
  username: "podlorenzo96",
  password: SFTP_PASSWORD,
};

const POLL_INTERVAL = 30000;
const MAX_ATTEMPTS = 3;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildFilename(row) {
  let name = row.filename || `claim_${row.id}.837`;
  if (row.is_test && !name.toUpperCase().includes("OATEST")) {
    const dotIndex = name.lastIndexOf(".");
    if (dotIndex > 0) {
      name = name.substring(0, dotIndex) + "_OATEST" + name.substring(dotIndex);
    } else {
      name = name + "_OATEST";
    }
  }
  return name;
}

async function processRow(row) {
  const attempt = (row.attempts || 0) + 1;
  const filename = buildFilename(row);
  const sftp = new SftpClient();

  console.log(`  Processing queue id=${row.id} attempt=${attempt} filename=${filename}`);

  try {
    await sftp.connect(SFTP_CONFIG);
    const buffer = Buffer.from(row.edi_content, "utf-8");
    await sftp.put(buffer, `/inbound/${filename}`);
    await sftp.end();

    const { error: updateErr } = await supabase
      .from("claim_submission_queue")
      .update({
        status: "submitted",
        attempts: attempt,
        submitted_at: new Date().toISOString(),
        error_message: null,
      })
      .eq("id", row.id);

    if (updateErr) {
      console.error(`    Queue update error: ${updateErr.message}`);
      return;
    }

    console.log(`    Uploaded ${filename} successfully`);

    if (row.claim_ids && row.claim_ids.length > 0) {
      const { error: claimErr } = await supabase
        .from("claim_records")
        .update({ status: "submitted" })
        .in("id", row.claim_ids);

      if (claimErr) console.error(`    Claim update error: ${claimErr.message}`);
      else console.log(`    Updated ${row.claim_ids.length} claim record(s)`);
    }
  } catch (err) {
    console.error(`    SFTP error: ${err.message}`);
    try { await sftp.end(); } catch (e) {}

    await supabase
      .from("claim_submission_queue")
      .update({
        status: "failed",
        attempts: attempt,
        error_message: err.message,
      })
      .eq("id", row.id);

    const backoffMs = Math.min(1000 * Math.pow(2, attempt), 30000);
    console.log(`    Backoff ${backoffMs}ms`);
    await sleep(backoffMs);
  }
}

async function pollQueue() {
  try {
    const { data: rows, error } = await supabase
      .from("claim_submission_queue")
      .select("*")
      .or("status.eq.pending,status.eq.failed")
      .lt("attempts", MAX_ATTEMPTS)
      .order("created_at", { ascending: true })
      .limit(20);

    if (error) {
      console.error("Queue fetch error:", error.message);
      return;
    }

    if (!rows || rows.length === 0) return;

    console.log(`[${new Date().toISOString()}] Processing ${rows.length} queue item(s)`);

    for (const row of rows) {
      await processRow(row);
    }
  } catch (err) {
    console.error("Poll error:", err.message);
  }
}

// ============================================================
// Outbound polling — pull 999 / 277CA / 277.txt acknowledgements
// from /outbound and POST to ingest-acks-officeally edge function.
// Dedup is DB-side: clearinghouse_ack_files.filename UNIQUE.
// We do NOT mutate or delete files on the SFTP server.
// ============================================================

const ACK_FILE_REGEX = /(_999\.999|_277(?:ca)?\.277|_277(?:ca)?\.txt)$/i;

async function pollOutbound() {
  if (!ACK_INGEST_URL || !ACK_INGEST_SHARED_SECRET) {
    console.log("[worker:ack] ACK_INGEST_URL or ACK_INGEST_SHARED_SECRET not configured - skipping outbound poll");
    return;
  }
  const sftp = new SftpClient();
  try {
    await sftp.connect({ ...SFTP_CONFIG, readyTimeout: 20000 });
    const list = await sftp.list(SFTP_OUTBOUND_DIR);
    const ackFiles = list.filter((f) => f.type === "-" && ACK_FILE_REGEX.test(f.name));
    if (ackFiles.length === 0) {
      console.log("[worker:ack] no ack files in /outbound");
      return;
    }

    const names = ackFiles.map((f) => f.name);
    const { data: known } = await supabase
      .from("clearinghouse_ack_files")
      .select("filename")
      .in("filename", names);
    const knownSet = new Set((known || []).map((r) => r.filename));
    const fresh = ackFiles.filter((f) => !knownSet.has(f.name));
    console.log(`[worker:ack] outbound: ${ackFiles.length} ack files, ${fresh.length} new`);

    for (const f of fresh) {
      try {
        const remote = `${SFTP_OUTBOUND_DIR}/${f.name}`;
        const buf = await sftp.get(remote);
        const content = Buffer.isBuffer(buf) ? buf : Buffer.from(buf);
        const res = await fetch(ACK_INGEST_URL, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "x-ack-shared-secret": ACK_INGEST_SHARED_SECRET,
          },
          body: JSON.stringify({
            filename: f.name,
            content_base64: content.toString("base64"),
          }),
        });
        const out = await res.json().catch(() => ({}));
        if (res.ok) console.log(`[worker:ack] ${f.name} -> ${JSON.stringify(out)}`);
        else console.error(`[worker:ack] ${f.name} ingest failed (${res.status}):`, out);
      } catch (err) {
        console.error(`[worker:ack] ${f.name} error:`, err.message);
      }
    }
  } catch (err) {
    console.error("[worker:ack] outbound poll error:", err.message);
  } finally {
    try { await sftp.end(); } catch {}
  }
}

console.log("poddispatch-sftp-worker started");
console.log(`Inbound queue polling every ${POLL_INTERVAL / 1000}s`);
console.log(`Outbound ack polling every ${OUTBOUND_POLL_INTERVAL_MS / 1000}s`);

(async function main() {
  while (true) {
    await pollQueue();
    await sleep(POLL_INTERVAL);
  }
})();

setInterval(pollOutbound, OUTBOUND_POLL_INTERVAL_MS);
pollOutbound();
