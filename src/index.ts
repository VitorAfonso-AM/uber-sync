import 'dotenv/config';
import SftpClient from 'ssh2-sftp-client';
import { parse } from 'csv-parse/sync';

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
  'Outras cobranças (moeda local)',
  'Status de Verificação',
];

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

// ================= MAIN =================

async function main() {
  console.log('🚀 Iniciando sync Uber');

  const sftp = new SftpClient();

  try {
    await sftp.connect(SFTP_CONFIG);

    const files = (await sftp.list(
      SFTP_CONFIG.remotePath
    )) as SftpFile[];

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
  } finally {
    await sftp.end();
  }
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error('💥 Erro:', err);
    process.exit(1);
  });