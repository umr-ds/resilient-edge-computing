# GitLab Runner Setup

The integration tests require Docker-in-Docker support, which is not available in standard GitLab Runner configurations due to security restrictions.
To run these tests, a self-hosted GitLab Runner with privileged mode enabled is needed.

## Setup

### 1. Start the Runner

```bash
docker compose -f gitlab-runner-compose.yml pull
docker compose -f gitlab-runner-compose.yml up -d
```

### 2. Register the Runner

After starting the container, the runner needs to be registered with the GitLab instance.

#### Get the registration token:

1. Go to the GitLab project
2. Navigate to **Settings -> CI/CD -> Runners**
3. Copy the registration token

#### Register the runner:

```bash
docker compose -f gitlab-runner-compose.yml exec gitlab-runner \
    gitlab-runner register \
    --non-interactive \
    --url "https://gitlab.uni-marburg.de/" \
    --token "TOKEN" \
    --executor "docker" \
    --docker-image "python:3.13" \
    --docker-privileged \
    --description "NAME (Privileged Runner)"
```

### 3. Configure Runner Tags in GitLab

During or after registration, assign tags to the runner:

1. Go to **Settings -> CI/CD -> Runners**
2. Find your newly registered runner
3. Click **Edit**
4. Add tags (e.g., `privileged`)
5. Optionally, uncheck "Run untagged jobs" to ensure only tagged jobs use this runner
6. Save changes

Jobs in `.gitlab-ci.yml` with matching tags will automatically run on this runner.
For example:

```yaml
pytest:
  stage: unit_test
  image: python:3.13
  tags:
    - privileged # This matches the runner tag
  services:
    - name: docker:27-dind
      alias: docker
  variables:
    DOCKER_HOST: tcp://docker:2375
    DOCKER_TLS_CERTDIR: ""
  before_script:
    - apt-get update
    - apt-get install -y ca-certificates curl
    - install -m 0755 -d /etc/apt/keyrings
    - curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
    - chmod a+r /etc/apt/keyrings/docker.asc
    - echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
    - apt-get update
    - apt-get install -y docker-ce-cli docker-compose-plugin
  script:
    - pip install --upgrade pip
    - pip install --group dev .
    - pip install --group test .
    - pytest
```
