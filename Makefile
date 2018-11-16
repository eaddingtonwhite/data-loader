# Go minimum version check.
GO_MIN_VERSION := 11000 # go1.10
GO_VERSION_CHECK := \
  $(shell expr \
    $(shell go version | \
      awk '{print $$3}' | \
      cut -do -f2 | \
      sed -e 's/\.\([0-9][0-9]\)/\1/g' -e 's/\.\([0-9]\)/0\1/g' -e 's/^[0-9]\{3,4\}$$/&00/' \
    ) \>= $(GO_MIN_VERSION) \
  )

# Default Go linker flags.
GO_LDFLAGS ?= -ldflags="-s -w"

ACCOUNT_ID ?= $(shell aws sts get-caller-identity | jq -r .Account)
ENVIRONMENT_NAME ?= dev
REVISION_ID := v0.1.0
MASTER_PASSWORD ?= $(shell  aws ssm get-parameter --name /data-loader/dev/master-password --with-decryption | jq .Parameter.Value)
DATA_LOADER := build/data-loader


.PHONY: all
all: check-go clean test $(DATA_LOADER)

# Build --------------------
$(DATA_LOADER):
	GOOS=linux GOARCH=amd64 go build $(GO_LDFLAGS) $(BUILDARGS) -o $@ ./functions/s3-upload/...
	zip -j $@.zip $@

.PHONY: vendor
vendor:
	dep ensure

.PHONY: clean
clean:
	@rm -rf build

.PHONY: check-go
check-go:
ifeq ($(GO_VERSION_CHECK),0)
	$(error go1.10 or higher is required)
endif

# Test ----------------------

.PHONY: test
test: check-go
	go test $(TESTARGS) -timeout=30s ./...
	@$(MAKE) vet
	@$(MAKE) lint

.PHONY: cover
cover: check-go
	@$(MAKE) test TESTARGS="-tags test -race -coverprofile=coverage.out"
	@go tool cover -html=coverage.out
	@rm -f coverage.out

# Lint ----------------------

.PHONY: vet
vet: check-go
	go vet $(VETARGS) ./...

.PHONY: lint
lint: check-go
	@echo "golint $(LINTARGS)"
	@for pkg in $(shell go list ./...) ; do \
		golint $(LINTARGS) $$pkg ; \
	done

# Local Testing/Dev -------

.PHONY: test-local
test-local:
	 sam local generate-event s3 put \
	 	--bucket test-databucket \
	 	--key testformat1_2015-06-28.txt | \
	 	sam local invoke \
	 	-t template_local.yaml \
	 	--env-vars test-data/env.json

# Deploy ------------------

.PHONY: upload-master-db-password
upload-master-db-password:
	aws ssm put-parameter \
		--name /data-loader/$(ENVIRONMENT_NAME)/master-password \
		--type SecureString \
		--overwrite \
		--value "$(MASTER_PASSWORD_TO_SET)"

.PHONY: create-artifact-bucket
create-artifact-bucket:
	aws s3 mb s3://artifacts-$(ACCOUNT_ID)-us-west-2 --region us-west-2

.PHONY: upload-artifacts
upload-artifacts:
	aws s3 sync ./infra s3://artifacts-$(ACCOUNT_ID)-us-west-2/data-loader/$(REVISION_ID)/ --region us-west-2
	aws s3 cp ./build/data-loader.zip s3://artifacts-$(ACCOUNT_ID)-us-west-2/data-loader/$(REVISION_ID)/ --region us-west-2

.PHONY: deploy
deploy:
	sam deploy \
		--template-file ./infra/template.yaml \
		--stack-name data-loader \
		--capabilities CAPABILITY_NAMED_IAM \
		--parameter-overrides \
		EnvironmentName=$(ENVIRONMENT_NAME) \
		ArtifactBucket=artifacts-$(ACCOUNT_ID)-us-west-2 \
		ArtifactFolder=data-loader/$(REVISION_ID) \
		MasterUserPassword=$(MASTER_PASSWORD)

