"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
require("dotenv/config");
const ssh2_sftp_client_1 = __importDefault(require("ssh2-sftp-client"));
const sync_1 = require("csv-parse/sync");
const node_cron_1 = __importDefault(require("node-cron"));
// ================= CONFIG =================
const SFTP_CONFIG = {
    host: 'sftp.uber.com',
    port: 2222,
    username: process.env.UBER_SFTP_USERNAME || '',
    privateKey: process.env.UBER_SFTP_PRIVATE_KEY?.replace(/\\n/g, '\n') || '',
    remotePath: '/from_uber/trips',
};
const SHEETS_API_URL = 'https://sheetsapi-4glvqxtnkq-uc.a.run.app';
const COLUMNS = [
    'ID da viagem/Uber Eats',
    'Registro de data e hora da transação (UTC)',
    'Data de chegada (UTC)',
    'Hora de chegada (UTC)',
    'Data de chegada (local)',
    'Hora de chegada (local)',
    'Nome',
    'Sobrenome',
    'Grupo',
    'Serviço',
    'Cidade',
    'País',
    'Distância (mi)',
    'Duração (min)',
    'Endereço de partida',
    'Endereço de destino',
    'Outras cobranças (moeda local)',
    'Status de Verificação',
];
// Horário do cron (padrão: 8h da manhã, horário de Brasília)
// Formato: minuto hora dia mês dia-da-semana
const CRON_SCHEDULE = process.env.CRON_SCHEDULE || '0 8 * * *';
// ================= FUNÇÕES =================
function getYesterdayFileName() {
    const d = new Date();
    d.setDate(d.getDate() - 1);
    return `daily_trips-${d.getFullYear()}_${String(d.getMonth() + 1).padStart(2, '0')}_${String(d.getDate()).padStart(2, '0')}.csv`;
}
function filterTrips(trips) {
    return trips.map((trip) => {
        const filtered = {};
        COLUMNS.forEach((col) => {
            filtered[col] =
                col === 'Status de Verificação'
                    ? 'Pendente'
                    : trip[col] || '';
        });
        return filtered;
    });
}
async function processCSVFile(sftp, fileName) {
    const remotePath = `${SFTP_CONFIG.remotePath}/${fileName}`;
    const buffer = await sftp.get(remotePath);
    const content = buffer.toString('utf-8');
    const lines = content.split('\n');
    const headerIndex = lines.findIndex((l) => l.toLowerCase().includes('id da viagem/uber eats'));
    if (headerIndex === -1)
        return [];
    return (0, sync_1.parse)(lines.slice(headerIndex).join('\n'), {
        columns: true,
        delimiter: ';',
        skip_empty_lines: true,
        trim: true,
    });
}
async function sendToGoogleSheets(trips) {
    if (!trips.length)
        return;
    const values = trips.map((t) => COLUMNS.map((c) => t[c] || ''));
    const res = await fetch(SHEETS_API_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ values }),
    });
    if (!res.ok) {
        throw new Error(`Sheets API error: ${res.status} ${res.statusText}`);
    }
}
// ================= SYNC FUNCTION =================
async function syncUberTrips() {
    const timestamp = new Date().toISOString();
    console.log(`\n[${timestamp}] 🚀 Iniciando sync Uber`);
    const sftp = new ssh2_sftp_client_1.default();
    try {
        await sftp.connect(SFTP_CONFIG);
        const files = (await sftp.list(SFTP_CONFIG.remotePath));
        const target = getYesterdayFileName();
        const file = files.find((f) => f.name === target);
        if (!file) {
            console.log(`[${timestamp}] ⚠️  Arquivo ${target} não encontrado`);
            return;
        }
        console.log(`[${timestamp}] 📥 Processando arquivo: ${file.name}`);
        const trips = await processCSVFile(sftp, file.name);
        const filtered = filterTrips(trips);
        console.log(`[${timestamp}] 📊 ${filtered.length} viagens encontradas`);
        await sendToGoogleSheets(filtered);
        console.log(`[${timestamp}] ✅ Sync concluído com sucesso`);
    }
    catch (error) {
        console.error(`[${timestamp}] 💥 Erro durante sync:`, error);
        throw error;
    }
    finally {
        await sftp.end();
    }
}
// ================= MAIN =================
async function main() {
    console.log('🔧 Uber Sync Service iniciado');
    console.log(`⏰ Agendamento: ${CRON_SCHEDULE}`);
    console.log(`🌍 Timezone: ${process.env.TZ || 'UTC'}`);
    // Validar configuração
    if (!SFTP_CONFIG.username || !SFTP_CONFIG.privateKey) {
        console.error('❌ UBER_SFTP_USERNAME e UBER_SFTP_PRIVATE_KEY são obrigatórios');
        process.exit(1);
    }
    // Executar imediatamente ao iniciar (opcional)
    if (process.env.RUN_ON_START === 'true') {
        console.log('🏃 Executando sync inicial...');
        try {
            await syncUberTrips();
        }
        catch (error) {
            console.error('❌ Erro no sync inicial:', error);
        }
    }
    // Configurar cron job
    node_cron_1.default.schedule(CRON_SCHEDULE, async () => {
        try {
            await syncUberTrips();
        }
        catch (error) {
            console.error('❌ Erro no cron job:', error);
        }
    }, {
        timezone: process.env.TZ || 'America/Sao_Paulo'
    });
    console.log('✅ Cron job configurado. Aguardando próxima execução...');
    // Manter o processo rodando
    process.on('SIGTERM', () => {
        console.log('👋 Recebido SIGTERM, encerrando...');
        process.exit(0);
    });
    process.on('SIGINT', () => {
        console.log('👋 Recebido SIGINT, encerrando...');
        process.exit(0);
    });
}
main().catch((err) => {
    console.error('💥 Erro fatal:', err);
    process.exit(1);
});
