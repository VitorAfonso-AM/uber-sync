FROM node:18-alpine

WORKDIR /app

# Instalar dependências do sistema
RUN apk add --no-cache tzdata

# Copiar package files
COPY package*.json ./

# Instalar dependências
RUN npm ci --only=production

# Copiar código fonte
COPY . .

# Compilar TypeScript
RUN npm run build

# Criar diretório para logs
RUN mkdir -p /app/logs

# Comando padrão (será sobrescrito pelo cron)
CMD ["node", "dist/index.js"]