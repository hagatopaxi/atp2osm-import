let currentInvalidItemId = null;
let invalidations = [];

function markSourceChecked(itemId) {
    document.querySelectorAll(`[data-validate-btn="${itemId}"]`).forEach((btn) => {
        btn.removeAttribute("disabled");
    });
    const warning = document.querySelector(`[data-source-warning="${itemId}"]`);
    if (warning) warning.remove();
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

  if (nextStepButton) {
    if (allValidated) {
      nextStepButton.removeAttribute("disabled");
      if (invalidations.length > 0) {
        const wikidata = extractWikidata(window.location.href);
        nextStepButton.href = `/brands/${wikidata}/rejected`;
        nextStepButton.classList.remove("btn-primary");
        nextStepButton.classList.add("btn-error");
        nextStepButton.addEventListener("click", () => {
          const brandName = document.querySelector("[data-brand-name]")?.dataset.brandName;
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
    osm_id: currentInvalidItemId,
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
