#!/bin/zsh
# Copy the page and publish the Worker. Needs: npx wrangler login, once.
cd "$(dirname "$0")"
cp ../src/dashboard.html public/index.html
npx --yes wrangler@4 deploy
