FROM node:8

# Create app directory
RUN mkdir -p /usr/src/app

COPY city-of-things-postgresql/ /usr/src/app
WORKDIR /usr/src/app

EXPOSE 8090
RUN npm install

CMD ["npm", "start"]