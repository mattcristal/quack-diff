// Custom JavaScript for quack-diff documentation.
//
// This follows the pattern from the Zensical docs:
// subscribe to `document$` so code runs on initial load
// and on subsequent instant navigation events.

document$.subscribe(function () {
  // Initialize any third-party libraries or page-specific behaviors here.
  console.log("quack-diff docs: custom JS initialized");
});
