FROM node:18

WORKDIR /app

ENV PORT=80

COPY package*.json ./

RUN npm install && npm install typescript -g

COPY src ./src
COPY tsconfig.json ./

# this must go after install or else dev deps will be ignored
ENV NODE_ENV=production

RUN npm run build

# Expose the port the app runs on
EXPOSE 80

CMD ["node", "dist/index.js"]
