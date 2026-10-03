// Sends the magic-link sign-in email. Uses Resend's plain HTTP API (no SDK
// dependency needed - Node 18+ has global fetch) when RESEND_API_KEY is
// set. Without it (the default for local dev), the link is printed to the
// console instead of actually emailed, so you can click it straight from
// your terminal without needing real email infrastructure running.
const RESEND_API_KEY = process.env.RESEND_API_KEY;
const MAGIC_LINK_FROM_EMAIL = process.env.MAGIC_LINK_FROM_EMAIL || 'login@xnlflow.com';

async function sendMagicLinkEmail(toEmail, link) {
  if (!RESEND_API_KEY) {
    console.log(`\n[mailer] RESEND_API_KEY not set - printing magic link instead of emailing it:`);
    console.log(`[mailer] To: ${toEmail}`);
    console.log(`[mailer] Link: ${link}\n`);
    return;
  }

  const res = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${RESEND_API_KEY}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      from: MAGIC_LINK_FROM_EMAIL,
      to: toEmail,
      subject: 'Sign in to XNL Flow',
      html: `
        <p>Click the link below to sign in to XNL Flow. This link expires in 15 minutes and can only be used once.</p>
        <p><a href="${link}">${link}</a></p>
        <p>If you didn't request this, you can safely ignore this email.</p>
      `,
    }),
  });

  if (!res.ok) {
    const detail = await res.text().catch(() => '');
    throw new Error(`Resend request failed (status ${res.status}): ${detail}`);
  }
}

module.exports = { sendMagicLinkEmail };
