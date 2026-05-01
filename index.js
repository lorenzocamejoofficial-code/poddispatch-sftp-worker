const { createClient } = require("@supabase/supabase-js");
const SftpClient = require("ssh2-sftp-client");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const SFTP_PASSWORD = process.env.SFTP_PASSWORD;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !SFTP_PASSWORD) {
  console.error("Missing required env vars");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

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

console.log("poddispatch-sftp-worker started");
console.log(`Polling every ${POLL_INTERVAL / 1000}s`);

(async function main() {
  while (true) {
    await pollQueue();
    await sleep(POLL_INTERVAL);
  }
})();
