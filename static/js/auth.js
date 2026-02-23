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
