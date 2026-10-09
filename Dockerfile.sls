FROM node:8.11.2

RUN npm install -g npm@6.1.0

ENV PATH=/app/node_modules/.bin:$PATH SLS_TRACKING_DISABLED=1

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci
