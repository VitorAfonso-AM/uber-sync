import 'dotenv/config';
import SftpClient from 'ssh2-sftp-client';
import { parse } from 'csv-parse/sync';
import cron from 'node-cron';
import { initializeApp, cert } from 'firebase-admin/app';
import { getFirestore } from 'firebase-admin/firestore';

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

// Inicializar Firebase
const serviceAccount = JSON.parse(
  process.env.FIREBASE_SERVICE_ACCOUNT || '{}'
);

initializeApp({
  credential: cert(serviceAccount),
});

const db = getFirestore();
const COLLECTION_NAME = 'uber_trips';

// Colunas originais necessárias para leitura
const SOURCE_COLUMNS = [
  'ID da viagem/Uber Eats',
  'Data da solicitação (local)',
  'Hora da solicitação (local)',
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
];

// Horário do cron (padrão: 8h da manhã, horário de Brasília)
const CRON_SCHEDULE = process.env.CRON_SCHEDULE || '0 8 * * *';

// Grupos a serem excluídos
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

async function sendToFirestore(trips: Trip[]) {
  if (!trips.length) return;

  const batch = db.batch();
  let batchCount = 0;
  const batches = [];

  for (const trip of trips) {
    const tripId = trip['ID da viagem/Uber Eats'];
    
    if (!tripId) continue;

    const docRef = db.collection(COLLECTION_NAME).doc(tripId);
    
    batch.set(docRef, {
      tripId: trip['ID da viagem/Uber Eats'],
      requestDate: trip['Data da solicitação (local)'],
      requestTime: trip['Hora da solicitação (local)'],
      arrivalTime: trip['Hora de chegada (local)'],
      fullName: trip['Nome Completo'],
      group: trip['Grupo'],
      service: trip['Serviço'],
      city: trip['Cidade'],
      country: trip['País'],
      distance: trip['Distância (mi)'],
      duration: trip['Duração (min)'],
      startAddress: trip['Endereço de partida'],
      endAddress: trip['Endereço de destino'],
      totalValue: trip['Valor total: BRL'],
      createdAt: new Date(),
      syncedAt: new Date(),
    });

    batchCount++;

    // Firestore batch tem limite de 500 operações
    if (batchCount === 500) {
      batches.push(batch.commit());
      batchCount = 0;
    }
  }

  // Commit do último batch se houver operações pendentes
  if (batchCount > 0) {
    batches.push(batch.commit());
  }

  await Promise.all(batches);
  console.log(`📝 ${trips.length} viagens salvas no Firestore`);
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

    console.log(`[${timestamp}] 📊 ${filtered.length} viagens encontradas (após filtros)`);
    await sendToFirestore(filtered);

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
  console.log(`📦 Collection: ${COLLECTION_NAME}`);
  console.log(`⏰ Agendamento: ${CRON_SCHEDULE}`);
  console.log(`🌍 Timezone: ${process.env.TZ || 'UTC'}`);
  console.log(`🚫 Grupos excluídos: ${EXCLUDED_GROUPS.join(', ')}`);

  // Validar configuração
  if (!SFTP_CONFIG.username || !SFTP_CONFIG.privateKey) {
    console.error('❌ UBER_SFTP_USERNAME e UBER_SFTP_PRIVATE_KEY são obrigatórios');
    process.exit(1);
  }

  if (!process.env.FIREBASE_SERVICE_ACCOUNT) {
    console.error('❌ FIREBASE_SERVICE_ACCOUNT é obrigatório');
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