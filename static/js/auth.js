if (new URLSearchParams(location.search).get('session_expired')) {
  history.replaceState(null, '', location.pathname);
  const c = document.createElement('div');
  c.className = 'toast toast-end toast-bottom z-50';
  c.innerHTML = '<div class="alert alert-warning text-sm shadow-md">Session expirée, veuillez vous reconnecter.</div>';
  document.addEventListener('DOMContentLoaded', () => { document.body.appendChild(c); setTimeout(() => c.remove(), 5000); });
}

async function login() {
  const res = await fetch("/login", { method: "POST" });
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
