.PHONY: docs-build docs-serve docs-test

docs-build:
	mdbook build docs

docs-serve:
	mdbook serve docs --open

docs-test:
	mdbook test docs
