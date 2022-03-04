.PHONY: default
default: | help

.PHONY: install-build-tools
install-build-tools: ## Install required tools for build/dev
	pip install wheel twine bump2version pipenv build

.PHONY: build
build: ## Build dist
	rm -rf *.egg-info
	rm -rf dist
	python -m build
	twine check dist/*

.PHONY: release
release: ## Release a new version, uploading it to PyPI
	@make build
	twine upload dist/*

.PHONY: bump-version-patch
bump-version-patch: ## Bump patch version, e.g. 0.0.1 -> 0.0.2
	bump2version patch

.PHONY: bump-version-minor
bump-version-minor: ## Bump minor version, e.g. 0.0.1 -> 0.1.0
	bump2version minor

.PHONY: bump-version-major
bump-version-minor: ## Bump major version, e.g. 0.0.1 -> 1.0.0
	bump2version major

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
