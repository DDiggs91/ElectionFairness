#!/usr/bin/env bash
set -euo pipefail

# Docker mounts named volumes as root-owned; hand them to the ubuntu user.
sudo chown -R ubuntu:ubuntu /home/ubuntu/.claude
sudo chown -R ubuntu:ubuntu /home/ubuntu/.ssh
chmod 700 /home/ubuntu/.ssh

SSH_KEY=/home/ubuntu/.ssh/id_ed25519
if [ ! -f "$SSH_KEY" ]; then
    ssh-keygen -t ed25519 -N "" -f "$SSH_KEY" -C "ubuntu@devcontainer"
    echo ""
    echo "=================================================================="
    echo " New SSH key generated. Add this PUBLIC key to GitHub:"
    echo " https://github.com/settings/ssh/new"
    echo "=================================================================="
    cat "${SSH_KEY}.pub"
    echo "=================================================================="
    echo ""
fi
chmod 600 "$SSH_KEY"
chmod 644 "${SSH_KEY}.pub"

# Trust GitHub's host key so the first git push/fetch doesn't prompt.
touch /home/ubuntu/.ssh/known_hosts
ssh-keyscan -t ed25519 github.com >> /home/ubuntu/.ssh/known_hosts 2>/dev/null
sort -u -o /home/ubuntu/.ssh/known_hosts /home/ubuntu/.ssh/known_hosts
