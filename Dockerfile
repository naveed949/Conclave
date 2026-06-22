# --- build stage: compile TypeScript to dist/ ---
FROM node:20 AS build
WORKDIR /app

# Install dependencies against the committed lockfile for reproducible builds.
COPY package.json yarn.lock ./
RUN yarn install --frozen-lockfile

# Build the project (tsc -> dist/).
COPY tsconfig.json ./
COPY src ./src
RUN yarn build

# Drop dev dependencies so only what the runtime needs is carried forward.
RUN yarn install --frozen-lockfile --production

# --- run stage: slim image with just the compiled output + prod deps ---
FROM node:20-slim AS run
WORKDIR /app
ENV NODE_ENV=production

# Each node writes its durable Raft state here (overridable via DATA_DIR).
ENV DATA_DIR=/app/data

COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY package.json ./

# HTTP port (overridden per node via the PORT env var / compose).
EXPOSE 3000

CMD ["node", "dist/server.js"]
