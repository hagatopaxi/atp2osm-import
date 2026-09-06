if (new URLSearchParams(location.search).get('session_expired')) {
  history.replaceState(null, '', location.pathname);
  const c = document.createElement('div');
  c.className = 'toast toast-end toast-bottom z-50';
  const alert = document.createElement('div');
  alert.className = 'alert alert-warning text-sm shadow-md';
  alert.textContent = t('session_expired');
  c.appendChild(alert);
  document.addEventListener('DOMContentLoaded', () => { document.body.appendChild(c); setTimeout(() => c.remove(), 5000); });
}

async function login(next = '/') {
  const res = await fetch("/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ next }),
  });
  const body = await res.text();
  window.location.replace(body);
}

async function logout() {
  await fetch("/logout", { method: "POST" });
  if (window.location.href === "/") {
    window.location.reload();
  } else {
    window.location.replace("/");
  }
}
