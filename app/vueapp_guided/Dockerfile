# Build step
FROM node:20-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
# Vite env vars are baked into the static bundle at BUILD time. This must be a
# URL the *browser* can reach (not a docker-internal hostname like router:8000).
# Unset/empty = the app falls back to http://localhost:8000.
ARG VITE_API_BASE=
ENV VITE_API_BASE=$VITE_API_BASE
RUN npm run build

# Production step: serve with nginx
FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
