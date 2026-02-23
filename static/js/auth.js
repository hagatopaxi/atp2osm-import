async function login() {
  const res = await fetch("/login", { method: "POST" });
  const body = await res.text();
  console.log(body);
  window.location.replace(body);
}
