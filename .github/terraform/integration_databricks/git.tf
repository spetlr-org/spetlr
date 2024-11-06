data "external" "git" {
  program = ["sh", "-c", <<-EOT
    echo '{ "root":"'$(git rev-parse --show-toplevel)'"}'
  EOT
  ]
}
