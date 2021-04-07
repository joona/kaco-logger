FROM node:14

WORKDIR /opt/kaco-logger

COPY package*.json ./
RUN npm install

COPY . .

CMD ["node", "index.js"]
