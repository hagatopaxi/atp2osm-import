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
  const errorBox = document.getElementById('form-error');

  errorBox.classList.add('hidden');

  const res = await fetch('/todo', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ brand_wikidata, brand_name, estimation }),
  });

  if (res.status === 201) {
    window.location.reload();
  } else {
    const data = await res.json();
    errorBox.textContent = data.error || 'Erreur lors de l\'ajout.';
    errorBox.classList.remove('hidden');
  }
}

async function deleteEntry(id) {
  if (!confirm('Supprimer cette entrée ?')) return;
  await fetch(`/todo/${id}`, { method: 'DELETE' });
  window.location.reload();
}
