import 'dotenv/config';
import SftpClient from 'ssh2-sftp-client';
import { parse } from 'csv-parse/sync';
import cron from 'node-cron';
import { db } from './firebase';

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

const FIRESTORE_COLLECTION = 'uber_trips';

const CRON_SCHEDULE = process.env.CRON_SCHEDULE || '0 8 * * *';

const EXCLUDED_GROUPS = ['ADMINISTRATIVO', 'COMERCIAL'];

// ================= FUNÇÕES =================

function getYesterdayFileName(): string {
  const d = new Date();
  d.setDate(d.getDate() - 1);

  return `daily_trips-${d.getFullYear()}_${String(
    d.getMonth() + 1
  ).padStart(2, '0')}_${String(d.getDate()).padStart(2, '0')}.csv`;
}

function filterTrips(trips: Trip[]): Trip[] {
  return trips
    .filter((trip) => {
      const grupo = trip['Grupo']?.trim() || '';
      return !EXCLUDED_GROUPS.includes(grupo);
    })
    .map((trip) => {
      const nome = trip['Nome']?.trim() || '';
      const sobrenome = trip['Sobrenome']?.trim() || '';
      const nomeCompleto = `${nome} ${sobrenome}`.trim();

      return {
        'ID da viagem/Uber Eats': trip['ID da viagem/Uber Eats'] || '',
        'Data da solicitação (local)': trip['Data da solicitação (local)'] || '',
        'Hora da solicitação (local)': trip['Hora da solicitação (local)'] || '',
        'Hora de chegada (local)': trip['Hora de chegada (local)'] || '',
        'Nome Completo': nomeCompleto,
        'Grupo': trip['Grupo'] || '',
        'Serviço': trip['Serviço'] || '',
        'Cidade': trip['Cidade'] || '',
        'País': trip['País'] || '',
        'Distância (mi)': trip['Distância (mi)'] || '',
        'Duração (min)': trip['Duração (min)'] || '',
        'Endereço de partida': trip['Endereço de partida'] || '',
        'Endereço de destino': trip['Endereço de destino'] || '',
        'Valor total: BRL': trip['Valor total: BRL'] || '',
      };
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

// ================= FIRESTORE =================

async function saveTripsToFirestore(trips: Trip[]) {
  if (!trips.length) return;

  const batch = db.batch();

  for (const trip of trips) {
    const tripId = trip['ID da viagem/Uber Eats'];
    if (!tripId) continue;

    const ref = db.collection(FIRESTORE_COLLECTION).doc(tripId);

    batch.set(
      ref,
      {
        tripId,
        requestDate: trip['Data da solicitação (local)'],
        requestTime: trip['Hora da solicitação (local)'],
        arrivalTime: trip['Hora de chegada (local)'],
        fullName: trip['Nome Completo'],
        group: trip['Grupo'],
        service: trip['Serviço'],
        city: trip['Cidade'],
        country: trip['País'],
        distanceMi: Number(trip['Distância (mi)']) || 0,
        durationMin: Number(trip['Duração (min)']) || 0,
        originAddress: trip['Endereço de partida'],
        destinationAddress: trip['Endereço de destino'],
        totalValueBRL:
          Number(trip['Valor total: BRL']?.replace(',', '.')) || 0,
        createdAt: new Date(),
      },
      { merge: true }
    );
  }

  await batch.commit();
}

// ================= SYNC =================

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
      console.log(`[${timestamp}] ⚠️ Arquivo ${target} não encontrado`);
      return;
    }

    console.log(`[${timestamp}] 📥 Processando arquivo: ${file.name}`);

    const trips = await processCSVFile(sftp, file.name);
    const filtered = filterTrips(trips);

    console.log(
      `[${timestamp}] 📊 ${filtered.length} viagens após filtros`
    );

    await saveTripsToFirestore(filtered);

    console.log(
      `[${timestamp}] ✅ Viagens salvas no Firestore`
    );
  } catch (error) {
    console.error(`[${timestamp}] 💥 Erro no sync:`, error);
    throw error;
  } finally {
    await sftp.end();
  }
}

// ================= MAIN =================

async function main() {
  console.log('🔧 Uber Sync Service iniciado');
  console.log(`⏰ Cron: ${CRON_SCHEDULE}`);
  console.log(`🚫 Grupos excluídos: ${EXCLUDED_GROUPS.join(', ')}`);

  if (!SFTP_CONFIG.username || !SFTP_CONFIG.privateKey) {
    console.error('❌ Credenciais SFTP ausentes');
    process.exit(1);
  }

  if (process.env.RUN_ON_START === 'true') {
    await syncUberTrips();
  }

  cron.schedule(
    CRON_SCHEDULE,
    async () => {
      try {
        await syncUberTrips();
      } catch (err) {
        console.error('❌ Erro no cron:', err);
      }
    },
    {
      timezone: process.env.TZ || 'America/Sao_Paulo',
    }
  );

  console.log('✅ Cron ativo. Aguardando execuções...');
}

main().catch((err) => {
  console.error('💥 Erro fatal:', err);
  process.exit(1);
}); 