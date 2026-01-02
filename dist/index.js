"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
var _a;
Object.defineProperty(exports, "__esModule", { value: true });
require("dotenv/config");
const ssh2_sftp_client_1 = __importDefault(require("ssh2-sftp-client"));
const sync_1 = require("csv-parse/sync");
// ================= CONFIG =================
const SFTP_CONFIG = {
    host: 'sftp.uber.com',
    port: 2222,
    username: process.env.UBER_SFTP_USERNAME || '',
    privateKey: ((_a = process.env.UBER_SFTP_PRIVATE_KEY) === null || _a === void 0 ? void 0 : _a.replace(/\\n/g, '\n')) || '',
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
// ================= MAIN =================
async function main() {
    console.log('🚀 Iniciando sync Uber');
    const sftp = new ssh2_sftp_client_1.default();
    try {
        await sftp.connect(SFTP_CONFIG);
        const files = (await sftp.list(SFTP_CONFIG.remotePath));
        const target = getYesterdayFileName();
        const file = files.find((f) => f.name === target);
        if (!file) {
            console.log(`Arquivo ${target} não encontrado`);
            return;
        }
        const trips = await processCSVFile(sftp, file.name);
        const filtered = filterTrips(trips);
        await sendToGoogleSheets(filtered);
        console.log('✅ Sync concluído');
    }
    finally {
        await sftp.end();
    }
}
main()
    .then(() => process.exit(0))
    .catch((err) => {
    console.error('💥 Erro:', err);
    process.exit(1);
});
//# sourceMappingURL=index.js.map