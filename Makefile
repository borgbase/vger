.PHONY: \
	docs-build \
	docs-serve \
	docs-test \
	fmt \
	fmt-check \
	lint \
	test \
	pre-commit

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

lint:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
	cargo test --workspace

pre-commit: fmt-check lint test

docs-build:
	mdbook build docs

docs-serve:
	mdbook serve docs --open

docs-test:
	mdbook test docs
