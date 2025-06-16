
#!/bin/bash
# Tear down cloud resources after benchmarks
TERRAFORM_DIR="$(pwd)/infra/aws"

cd "$TERRAFORM_DIR" || exit 1
terraform destroy -auto-approve

# Clean up S3 artifacts
aws s3 rm s3://datafusion-benchmarks/ --recursive
