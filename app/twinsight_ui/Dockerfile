FROM node:20-alpine AS build
WORKDIR /app
ARG VITE_UI_PRIMARY=false
ARG VITE_SERVICE_MODE=mock
ENV VITE_UI_PRIMARY=$VITE_UI_PRIMARY
ENV VITE_SERVICE_MODE=$VITE_SERVICE_MODE
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
