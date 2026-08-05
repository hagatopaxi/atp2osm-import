function extractWikidata(url) {
  const parts = url.split("/");
  return parts.find((part) => /^Q\d+$/.test(part));
}

function renderInvalidations() {
  const container = document.getElementById("invalidations_list");
  const data = JSON.parse(sessionStorage.getItem("invalidations") || "[]");

  if (data.length === 0) {
    container.innerHTML =
      '<p class="text-base-content/60">Aucune invalidation trouvée.</p>';
    return;
  }

  for (const item of data) {
    const card = document.createElement("div");
    card.className =
      "card bg-base-100 border-error border shadow-md p-4 flex flex-row items-start gap-4";
    card.innerHTML = `
      <i class="iconoir-prohibition text-error text-2xl mt-1"></i>
      <div>
        <p class="font-semibold">${item.title}</p>
        ${item.comment ? `<p class="text-base-content/70 mt-1">${item.comment}</p>` : ""}
        ${item.atp_id ? `<p class="text-xs text-base-content/50 font-mono mt-1">ATP ${item.spider_id ? item.spider_id + "/" : ""}${item.atp_id}</p>` : ""}
      </div>
    `;
    container.appendChild(card);
  }
}

async function confirmRejection() {
  const wikidata = extractWikidata(window.location.href);
  const data = JSON.parse(sessionStorage.getItem("invalidations") || "[]");
  const confirmBtn = document.getElementById("confirm_btn");
  const loading = document.getElementById("loading");

  confirmBtn.setAttribute("disabled", true);
  loading.classList.remove("hidden");

  const comment = data.map((item) => ({
    osm_id: item.osm_id,
    osm_type: item.osm_type,
    atp_id: item.atp_id,
    spider_id: item.spider_id,
    comment: item.comment,
  }));

  try {
    const res = await fetch(`/brands/${wikidata}/report-error`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ comment: JSON.stringify(comment), brand_name: sessionStorage.getItem("brand_name") || "" }),
    });

    if (!res.ok) throw new Error(res.statusText);

    const data = await res.json();
    sessionStorage.removeItem("invalidations");
    sessionStorage.removeItem("brand_name");
    window.location.href = `/history/${data.id}`;
  } catch (err) {
    alert("Erreur lors de l'envoi : " + err.message);
    confirmBtn.removeAttribute("disabled");
    loading.classList.add("hidden");
  }
}

document.addEventListener("DOMContentLoaded", renderInvalidations);
