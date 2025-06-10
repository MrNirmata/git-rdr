FROM node:20-alpine

WORKDIR /app

# დამოკიდებულებების დაყენება
COPY package*.json ./
RUN npm install

# კოდის კოპირება
COPY . .

# Next.js-ის პორტი
EXPOSE 3000

# დეველოპმენტ სერვერის გაშვება
CMD ["npm", "run", "dev"]