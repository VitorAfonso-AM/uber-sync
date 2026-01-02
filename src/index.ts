import 'dotenv/config';
import SftpClient from 'ssh2-sftp-client';
import { parse } from 'csv-parse/sync';
import cron from 'node-cron';

// ================= TIPOS =================

type Trip = Record<string, string>;

interface SftpFile {
  name: string;
  type: string;
  size: number;
  modifyTime: number;
  accessTime: number;
  rights: {
    user: string;
    group: string;
    other: string;
  };
  owner: number;
  group: number;
}

// ================= CONFIG =================

const SFTP_CONFIG = {
  host: 'sftp.uber.com',
  port: 2222,
  username: process.env.UBER_SFTP_USERNAME || '',
  privateKey: process.env.UBER_SFTP_PRIVATE_KEY?.replace(/\\n/g, '\n') || '',
  remotePath: '/from_uber/trips',
};

const SHEETS_API_URL =
  'https://sheetsapi-4glvqxtnkq-uc.a.run.app';

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
  'Valor total: BRL',
  'Status de Verificação',
];

// Horário do cron (padrão: 8h da manhã, horário de Brasília)
// Formato: minuto hora dia mês dia-da-semana
const CRON_SCHEDULE = process.env.CRON_SCHEDULE || '0 8 * * *';

// ================= FUNÇÕES =================

function getYesterdayFileName(): string {
  const d = new Date();
  d.setDate(d.getDate() - 1);

  return `daily_trips-${d.getFullYear()}_${String(
    d.getMonth() + 1
  ).padStart(2, '0')}_${String(d.getDate()).padStart(2, '0')}.csv`;
}

function filterTrips(trips: Trip[]): Trip[] {
  return trips.map((trip) => {
    const filtered: Trip = {};
    COLUMNS.forEach((col) => {
      filtered[col] =
        col === 'Status de Verificação'
          ? 'Pendente'
          : trip[col] || '';
    });
    return filtered;
  });
}

async function processCSVFile(
  sftp: InstanceType<typeof SftpClient>,
  fileName: string
): Promise<Trip[]> {
  const remotePath = `${SFTP_CONFIG.remotePath}/${fileName}`;
  const buffer = await sftp.get(remotePath);
  const content = buffer.toString('utf-8');

  const lines = content.split('\n');
  const headerIndex = lines.findIndex((l) =>
    l.toLowerCase().includes('id da viagem/uber eats')
  );

  if (headerIndex === -1) return [];

  return parse(lines.slice(headerIndex).join('\n'), {
    columns: true,
    delimiter: ';',
    skip_empty_lines: true,
    trim: true,
  }) as Trip[];
}

async function sendToGoogleSheets(trips: Trip[]) {
  if (!trips.length) return;

  const values = trips.map((t) =>
    COLUMNS.map((c) => t[c] || '')
  );

  const res = await fetch(SHEETS_API_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ values }),
  });

  if (!res.ok) {
    throw new Error(
      `Sheets API error: ${res.status} ${res.statusText}`
    );
  }
}

// ================= SYNC FUNCTION =================

async function syncUberTrips() {
  const timestamp = new Date().toISOString();
  console.log(`\n[${timestamp}] 🚀 Iniciando sync Uber`);

  const sftp = new SftpClient();

  try {
    await sftp.connect(SFTP_CONFIG);

    const files = (await sftp.list(
      SFTP_CONFIG.remotePath
    )) as SftpFile[];

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
  } catch (error) {
    console.error(`[${timestamp}] 💥 Erro durante sync:`, error);
    throw error;
  } finally {
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
    } catch (error) {
      console.error('❌ Erro no sync inicial:', error);
    }
  }

  // Configurar cron job
  cron.schedule(CRON_SCHEDULE, async () => {
    try {
      await syncUberTrips();
    } catch (error) {
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