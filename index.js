import express from 'express';
import multer from 'multer';
import PDFParser from 'pdf2json';
import { z } from 'zod';
import OpenAI from 'openai';
import dotenv from 'dotenv';
import { v4 as uuidv4 } from 'uuid';
import cors from 'cors';

// Load environment variables
dotenv.config();
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
if (!OPENAI_API_KEY) {
  throw new Error('OPENAI_API_KEY is not set');
}

// Initialize OpenAI
const openai = new OpenAI({
  apiKey: OPENAI_API_KEY,
});

// Initialize Express app
const app = express();

// Enable CORS with specific origins
app.use(cors({
  origin: [
    'http://cmom.leathree.com', // Replace with your PHP app's Heroku URL
    'http://localhost:3001', // Allow local development
    'http://localhost:3000'  // Existing local origin (if needed)
  ],
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Accept'],
  credentials: true // Allow credentials if needed (e.g., cookies)
}));

// Handle CORS preflight requests explicitly
app.options('*', cors());

const upload = multer({ storage: multer.memoryStorage() });

// In-memory job storage (for simplicity; use Redis or DB for production)
const jobs = new Map(); // jobId -> { status: 'pending' | 'completed' | 'error', data: [], error: string }

// Unified row schema using zod
const UnifiedRowSchema = z.object({
  Name: z.string().nullable().optional(),
  MemberID: z.string().nullable().optional(),
  T1023AuthId: z.string().nullable().optional(),
  T1023Range: z.string().nullable().optional(),
  T1023BillDate: z.string().nullable().optional(),
  H0044AuthId: z.string().nullable().optional(),
  H0044Range: z.string().nullable().optional(),
  H0044BillDate: z.string().nullable().optional(),
  Paid: z.string().nullable().optional(),
});

// Express routes
app.get('/debug', async (req, res) => {
  res.json({ openai_version: OpenAI.version || 'unknown' });
});

app.get('/', async (req, res) => {
  res.json({ status: true, message: 'Billing PDF API is running 🚀' });
});

app.post('/extract', upload.single('file'), async (req, res) => {
  if (!req.file || !req.file.mimetype.endsWith('pdf')) {
    console.error('Invalid file upload:', { mimetype: req.file?.mimetype });
    return res.status(400).json({ status: false, data: [], error: 'Please upload a PDF' });
  }

  const jobId = uuidv4();
  jobs.set(jobId, { status: 'pending' });
  console.log(`Started job ${jobId} for PDF processing`);

  // Process in background to avoid timeout
  setImmediate(async () => {
    try {
      // 1) Extract text per page
      let allTextLines = [];
      const pdfParser = new PDFParser();
      const pdfData = await new Promise((resolve, reject) => {
        pdfParser.on('pdfParser_dataError', errData => reject(errData));
        pdfParser.on('pdfParser_dataReady', pdfData => resolve(pdfData));
        pdfParser.parseBuffer(req.file.buffer);
      });

      // Extract text from pdf2json output
      allTextLines = pdfData.Pages
        .flatMap(page => {
          // Group texts by Y-coordinate to approximate table rows
          const textsByRow = {};
          page.Texts.forEach(text => {
            const y = text.y;
            if (!textsByRow[y]) textsByRow[y] = [];
            try {
              textsByRow[y].push({ x: text.x, text: decodeURIComponent(text.R[0].T) });
            } catch (e) {
              console.error(`Failed to decode text: ${text.R[0].T}`);
            }
          });
          // Sort texts by X-coordinate and join into a line
          return Object.values(textsByRow)
            .map(row => row.sort((a, b) => a.x - b.x).map(item => item.text).join(' ').replace(/\s+/g, ' ').trim())
            .filter(line => line);
        });

      console.log(`Job ${jobId} - Extracted text lines:`, allTextLines);

      if (!allTextLines.length) {
        jobs.set(jobId, { status: 'error', error: 'No text extracted from PDF' });
        console.error(`Job ${jobId} - No text extracted`);
        return;
      }

      // 2) Heuristic: filter rows (relaxed)
      const stopMarkers = [
        "AP'S OVERDUE",
        "AP'S DUE",
        "OVERDUE AP",
        "DUE CM",
        "SEPTEMBER",
        "ALL INTAKE",
        "NEEDS H0044",
      ];
      let filteredLines = [];
      for (const line of allTextLines) {
        if (stopMarkers.some(marker => line.toUpperCase().includes(marker))) {
          console.log(`Job ${jobId} - Stopped filtering at marker:`, line);
          break;
        }
        // Include potential header or data lines
        if (
          line.toUpperCase().includes('NAME') ||
          line.toUpperCase().includes('MRN') ||
          line.toUpperCase().includes('MBR') ||
          line.toUpperCase().includes('T1023') ||
          line.toUpperCase().includes('H0044') ||
          /^\d+\s/.test(line) ||
          line.includes(',') ||
          /\d{2}\/\d{2}/.test(line) ||
          /[A-Z0-9]{5,}/.test(line) // Potential auth IDs
        ) {
          filteredLines.push(line);
        }
      }

      if (!filteredLines.length) {
        console.log(`Job ${jobId} - No lines passed filtering, using all text lines`);
        filteredLines = allTextLines;
      }

      console.log(`Job ${jobId} - Filtered lines:`, filteredLines);

      // 3) Build AI prompt
      const systemPrompt = `
        You are a precise information extraction engine for billing tables.
        You will receive text lines from a PDF, which may include headers and rows.
        The lines may be fragmented, incomplete, or lack clear structure.
        Group lines into logical table rows based on context and map them to the schema below.
        Return ONLY valid JSON of the form: {"rows": [ ... ]} with NO extra text.
        Ensure the output is valid JSON, even if no rows are extracted.
      `;

      const userInstructions = `
We have billing tables from different insurers with varying headers.
The input lines may be fragmented (e.g., names, IDs, or dates on separate lines).
Group lines into logical rows and unify each row into this MERGED SCHEMA (use strings; use null if missing):

- Name
- MemberID (from MRN#, MRN, or MBR ID #)
- T1023AuthId
- T1023Range
- T1023BillDate
- H0044AuthId
- H0044Range
- H0044BillDate
- Paid

IMPORTANT RULES:
1) Group lines into rows based on context (e.g., a name followed by an ID or date).
2) Pair RANGE/BILL columns correctly for T1023 and H0044.
3) 'MemberID' comes from MRN#, MRN, or MBR ID # (alphanumeric IDs).
4) Do not invent data. Use null for missing fields.
5) Keep date/range formats as found (e.g., '04/01-07/01' or '04/01/23').
6) Use context clues (e.g., date patterns 'MM/DD' or 'MM/DD/YYYY', alphanumeric IDs) to align data.
7) If no valid rows can be formed, return {"rows": []}.
8) Ensure the output is valid JSON with properly escaped strings.

LINES:
${JSON.stringify(filteredLines)}
`;

      const completion = await openai.chat.completions.create({
        model: 'gpt-4o-mini',
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: userInstructions },
        ],
        response_format: { type: 'json_object' },
        temperature: 0,
        max_tokens: 4000,
      });

      const content = completion.choices[0].message.content;
      console.log(`Job ${jobId} - OpenAI response:`, content);

      let data;
      try {
        data = JSON.parse(content);
      } catch (jsonError) {
        console.error(`Job ${jobId} - JSON parse error:`, jsonError.message);
        console.error(`Job ${jobId} - Raw OpenAI response:`, content);
        jobs.set(jobId, { status: 'error', error: `Invalid JSON response: ${jsonError.message}` });
        return;
      }

      const rows = data.rows || [];

      // Validate and normalize rows
      const normalized = rows.map(row => {
        const parsed = UnifiedRowSchema.safeParse(row);
        return parsed.success ? parsed.data : {};
      });

      jobs.set(jobId, { status: 'completed', data: normalized });
      console.log(`Job ${jobId} - Completed with ${normalized.length} rows`);
    } catch (e) {
      console.error(`Job ${jobId} - Processing error:`, e);
      jobs.set(jobId, { status: 'error', error: e.message });
    }
  });

  res.json({ status: true, jobId, message: 'Processing started. Poll /status/' + jobId });
});

app.get('/status/:jobId', (req, res) => {
  const jobId = req.params.jobId;
  const job = jobs.get(jobId);
  if (!job) {
    console.error(`Status check failed for job ${jobId}: Job not found`);
    return res.status(404).json({ error: `Job ${jobId} not found` });
  }

  console.log(`Status check for job ${jobId}: ${job.status}`);
  res.json(job.status === 'completed' ? { status: true, data: job.data } :
           job.status === 'error' ? { status: 'error', error: job.error } :
           { status: 'pending' });
});

// Start server
const PORT = process.env.PORT || 8000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});