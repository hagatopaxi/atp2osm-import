let currentInvalidItemId = null;
let invalidations = [];
let apiAnswered = false;

function markSourceChecked(itemId) {
  document
    .querySelectorAll(`[data-validate-btn="${itemId}"]`)
    .forEach((btn) => {
      btn.removeAttribute("disabled");
    });
  const warning = document.querySelector(`[data-source-warning="${itemId}"]`);
  if (warning) warning.remove();

  // La première fois qu'une source est ouverte, on demande si c'est une API :
  // on remplace le bouton par la question. Une fois répondu, on ne repose plus.
  if (apiAnswered) return;
  const sourceBtn = document.querySelector(`[data-source-btn="${itemId}"]`);
  if (sourceBtn) sourceBtn.classList.add("hidden");
  const question = document.querySelector(`[data-api-question="${itemId}"]`);
  if (question) question.classList.remove("hidden");
}

// Réponse à « est-ce une API ? » : on fige le message dans le bandeau et, si
// oui, on débloque les points restants.
function answerIsApi(btn, isApi) {
  apiAnswered = true;
  const question = btn.closest("[data-api-question]");
  question.querySelector("[data-api-msg]").textContent = isApi
    ? "Cette source de données est une API."
    : "Cette source de données n'est pas une API.";
  question.querySelector("[data-api-choices]").remove();
  if (isApi) markBrandIsApi();
}

// La source est une API : elle n'est plus obligatoire, on débloque les points
// restants, on retire les avertissements et on remplace les boutons « ouvrir
// la source » restants par une simple mention.
function markBrandIsApi() {
  document
    .querySelectorAll("[data-validate-btn][disabled]")
    .forEach((btn) => btn.removeAttribute("disabled"));
  document
    .querySelectorAll("[data-source-warning]")
    .forEach((warning) => warning.remove());
  document.querySelectorAll("[data-source-btn]:not(.hidden)").forEach((btn) => {
    const mention = document.createElement("div");
    mention.className = "alert alert-soft alert-info";
    mention.innerHTML =
      '<i class="iconoir-database"></i><span>Cette source de données est une API.</span>';
    btn.replaceWith(mention);
  });
}

function extractWikidata(url) {
  const parts = url.split("/");
  return parts.find((part) => /^Q\d+$/.test(part));
}

function validateData(itemId) {
  const collapse = document.querySelector(`[data-item-id="${itemId}"]`);
  if (collapse) {
    collapse.classList.add("border-success", "bg-success/10", "validated");
    collapse.querySelector(".content").classList.add("hidden");
    checkAllValidated();
  }
}

function invalidateData(itemId) {
  currentInvalidItemId = itemId;
  document.getElementById("invalidation_comment").value = "";
  document.getElementById("invalidation_modal").showModal();
}

function checkAllValidated() {
  const cards = document.querySelectorAll("[data-item-id]");
  const nextStepButton = document.querySelector("a.nextStep");

  const allValidated = Array.from(cards).every((card) =>
    card.classList.contains("validated"),
  );
  console.log(cards, allValidated, nextStepButton);
  if (nextStepButton) {
    if (allValidated) {
      nextStepButton.removeAttribute("disabled");
      if (invalidations.length > 0) {
        const wikidata = extractWikidata(window.location.href);
        nextStepButton.href = `/brands/${wikidata}/rejected`;
        nextStepButton.classList.remove("btn-primary");
        nextStepButton.classList.add("btn-error");
        nextStepButton.addEventListener("click", () => {
          const brandName =
            document.querySelector("[data-brand-name]")?.dataset.brandName;
          sessionStorage.setItem(
            "invalidations",
            JSON.stringify(invalidations),
          );
          sessionStorage.setItem("brand_name", brandName || "");
        });
      }
    } else {
      nextStepButton.setAttribute("disabled", true);
    }
  }
}

function publishComment() {
  const comment = document.getElementById("invalidation_comment").value;

  const collapse = document.querySelector(
    `[data-item-id="${currentInvalidItemId}"]`,
  );
  const title = collapse
    ? collapse.querySelector(".title").textContent.trim()
    : currentInvalidItemId;
  const nodeType = collapse ? collapse.dataset.nodeType : null;

  invalidations.push({
    osm_id: collapse ? parseInt(collapse.dataset.osmId) : null,
    osm_type: nodeType,
    title,
    comment,
  });

  document.getElementById("invalidation_modal").close();

  if (collapse) {
    collapse.classList.add("border-error", "bg-error/10", "validated");
    collapse.querySelector(".content").classList.add("hidden");
    checkAllValidated();
  }

  currentInvalidItemId = null;
}
