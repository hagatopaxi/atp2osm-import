function validateData(itemId) {
  const collapse = document.querySelector(`[data-item-id="${itemId}"]`);
  if (collapse) {
    collapse.classList.add("border-success", "bg-success/10", "validated");
    collapse.querySelector(".content").classList.add("hidden");
    checkAllValidated();
  }
}

function invalidateData(itemId) {
  // TODO
}

function checkAllValidated() {
  const cards = document.querySelectorAll("[data-item-id]");
  const nextStepButton = document.querySelector(".nextStep");

  const allValidated = Array.from(cards).every((card) =>
    card.classList.contains("validated"),
  );

  if (nextStepButton) {
    nextStepButton.disabled = !allValidated;
  }
}

function publishComment() {}
