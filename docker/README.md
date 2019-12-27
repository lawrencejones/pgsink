# stolon-pgbouncer/docker

This directory contains docker images that are used in the stolon-pgbouncer
project. All these images are tagged as YYYYMMDDXX where XX is an index of build
for that day.

You can find Makefile targets to build and push these images in the root of the
project.

## base

This provides a common set of installed utilities that are shared between every
image, including our release images. You can expect any critical
stolon-pgbouncer dependencies (such as PgBouncer) to be installed in this image.

## circleci

We run unit, integration and acceptance tests in CircleCI. These test steps
often require more dependencies than are normal to put in a base image. Ideally
CircleCI should do no dependency installation, and all steps that execute in
containers should be run with this base image.

## stolon-development

Our docker-compose setup relies on this image to boot the different machine
roles, such as the supervised pgbouncer and keepers. Running `docker-compose
build` will generate this image.
