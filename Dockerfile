FROM node:18-alpine

WORKDIR /app

# Instalar dependências do sistema
RUN apk add --no-cache tzdata

# Copiar package files
COPY package*.json ./

# Instalar TODAS as dependências (incluindo devDependencies para build)
RUN npm ci

# Copiar código fonte
COPY . .

# Compilar TypeScript
RUN npm run build

# Remover devDependencies depois do build
RUN npm prune --production

# Criar diretório para logs
RUN mkdir -p /app/logs

# Comando padrão (será sobrescrito pelo cron)
CMD ["node", "dist/index.js"]