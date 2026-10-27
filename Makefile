qa: analyze test

analyze:
	@cargo clippy --workspace --all-features --all-targets
	@cargo fmt --all --check
	@cargo doc --workspace --all-features --no-deps --document-private-items

test:
	@cargo test --workspace --all-features

format:
	@cargo fmt --all

.PHONY: analyze format qa test
