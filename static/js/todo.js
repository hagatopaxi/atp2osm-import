function showToast(message, type = 'error') {
  const container = document.createElement('div');
  container.className = 'toast toast-end toast-bottom z-50';
  const alert = document.createElement('div');
  alert.className = `alert alert-${type} text-sm shadow-md`;
  alert.textContent = message;
  container.appendChild(alert);
  document.body.appendChild(container);
  setTimeout(() => container.remove(), 4000);
}

async function checkDuplicate() {
  const wikidata = document.getElementById('input-wikidata').value.trim();
  const name = document.getElementById('input-name').value.trim();
  const warning = document.getElementById('duplicate-warning');
  const warningText = document.getElementById('duplicate-warning-text');

  if (!wikidata && !name) {
    warning.classList.add('hidden');
    return;
  }

  const params = new URLSearchParams();
  if (wikidata) params.set('wikidata', wikidata);
  if (name) params.set('name', name);

  const res = await fetch('/todo/check?' + params.toString());
  const data = await res.json();

  if (data.matches && data.matches.length > 0) {
    const names = data.matches.map(m => `${m.brand_name} (${m.brand_wikidata})`).join(', ');
    warningText.textContent = `Attention : cette marque semble déjà présente — ${names}`;
    warning.classList.remove('hidden');
  } else {
    warning.classList.add('hidden');
  }
}

async function addEntry(event) {
  event.preventDefault();
  const brand_wikidata = document.getElementById('input-wikidata').value.trim();
  const brand_name = document.getElementById('input-name').value.trim();
  const estimationRaw = document.getElementById('input-estimation').value.trim();
  const estimation = estimationRaw !== '' ? parseInt(estimationRaw, 10) : null;

  const btn = event.target.querySelector('button[type="submit"]');
  btn.disabled = true;
  btn.classList.add('loading', 'loading-spinner');

  const res = await fetch('/todo', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ brand_wikidata, brand_name, estimation }),
  });

  if (res.status === 201) {
    document.getElementById('input-wikidata').value = '';
    document.getElementById('input-name').value = '';
    document.getElementById('input-estimation').value = '';
    document.getElementById('duplicate-warning').classList.add('hidden');
    window.location.reload();
  } else {
    btn.disabled = false;
    btn.classList.remove('loading', 'loading-spinner');
    let message = 'Une erreur est survenue, veuillez réessayer.';
    try {
      const data = await res.json();
      if (data.error) message = data.error;
    } catch (_) {}
    showToast(message);
  }
}

async function deleteEntry(id) {
  if (!confirm('Supprimer cette entrée ?')) return;
  const res = await fetch(`/todo/${id}`, { method: 'DELETE' });
  if (res.ok) {
    window.location.reload();
  } else {
    showToast(res.status === 403 ? 'Vous ne pouvez supprimer que vos propres entrées.' : 'Erreur lors de la suppression.');
  }
}
