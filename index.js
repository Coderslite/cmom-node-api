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
    'http://cmom.leathree.com',
    'https://cmom.leathree.com',
    'http://localhost:3001',
    'http://localhost:3000'
  ],
  methods: ['GET', 'POST', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Accept'],
  credentials: true
}));

app.options('*', cors());

const upload = multer({ storage: multer.memoryStorage() });

// In-memory job storage
const jobs = new Map();

// Unified row schema
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

  setImmediate(async () => {
    try {
      // 1) Extract structured rows with positions
      const pdfParser = new PDFParser();
      const pdfData = await new Promise((resolve, reject) => {
        pdfParser.on('pdfParser_dataError', errData => reject(errData));
        pdfParser.on('pdfParser_dataReady', pdfData => resolve(pdfData));
        pdfParser.parseBuffer(req.file.buffer);
      });

      const pageRows = pdfData.Pages.map(page => {
        const textsByRow = {};
        page.Texts.forEach(text => {
          const y = text.y;
          if (!textsByRow[y]) textsByRow[y] = [];
          let decoded;
          try {
            decoded = decodeURIComponent(text.R[0].T);
          } catch (e) {
            console.error(`Failed to decode text: ${text.R[0].T}`);
            decoded = text.R[0].T; // fallback
          }
          textsByRow[y].push({ x: text.x, text: decoded });
        });
        // Sort rows by y (top to bottom)
        const sortedY = Object.keys(textsByRow).sort((a, b) => parseFloat(a) - parseFloat(b));
        return sortedY.map(y => textsByRow[y].sort((a, b) => a.x - b.x));
      });

      console.log(`Job ${jobId} - Extracted page rows (length): ${pageRows.map(p => p.length)}`);

      if (!pageRows.flat().length) {
        throw new Error('No text extracted from PDF');
      }

      const inputData = JSON.stringify(pageRows);

      // 2) AI prompts for position-aware table extraction
      const systemPrompt = `
        You are a precise information extraction engine for billing tables from PDFs.
        The input is a JSON array of pages, each page an array of rows, each row an array of {x: number, text: string} sorted by x (left to right).
        Rows are sorted top to bottom per page.
        Parse the main billing table (before "AP'S OVERDUE" or similar) into structured rows using positions to align columns.
        Return ONLY valid JSON: {"rows": [ ... ]}. No extra text.
      `;

      const userInstructions = `
        Input is JSON of pageRows. To extract:

        1. Combine all pages' rows into one list (append page rows).

        2. Find table start: rows with texts like "NAME", "MRN#", "MBR ID #", "T1023", "H0044" (headers may span multiple rows).

        3. Find data rows: following rows starting with small number (e.g., "1", "2") as first text.

        4. Stop at rows with "AP'S OVERDUE", "AP'S DUE", "OVERDUE", "DUE CM", "ALL INTAKE", "NEEDS H0044", "CM".

        5. Collect tableRows = header rows + data rows.

        6. Identify columns: Collect all unique x from tableRows, sort them. Group close x (diff < 0.5) into one column, use min x as column key.

        Let columnPositions = sorted array of those min x.

        7. For each row in tableRows, create columnValues: array of length columnPositions.length, init empty strings.

        For each text in row, find closest column i where |text.x - columnPositions[i]| is min, append text to columnValues[i] with space.

        8. Determine headers: For header rows (before first data row), combine columnValues[i] across them (join with ' ') to get label[i].

        Trim and normalize labels (e.g., "HOO44" -> "H0044").

        9. Map labels to schema fields:

        - "NAME" -> Name

        - "MRN#" or "MBR ID #" or "MRN" -> MemberID

        - "DOB" -> skip

        - First "T1023" label column -> T1023AuthId

        - Next column if includes "AUTH DATES" or "DATE RANGE" or "D/R" or "B/D RANGE" -> T1023Range

        - Next if "BILLED" or "BILL DATE" -> T1023BillDate

        - "H0044" -> H0044AuthId

        - Next "AUTH DATES" or "D/R" -> H0044Range

        - Next "BILLED" -> H0044BillDate

        - "PAID" -> Paid

        Match loosely, use position sequence.

        10. For each data row, get columnValues, then create object with mapped fields = columnValues[corresponding i], trim, null if empty.

        For Name field: If columnValues[Name index] starts with a number (e.g., "16 Villasenor, Casandra"), remove the number and any following space to get only the name (e.g., "Villasenor, Casandra").

        Return {"rows": array of those objects}

        If no rows, {"rows": []}

        Example (simplified x):

        Suppose rows:

        [[{x:5, text:"NAME"}, {x:20, text:"MRN#"}, {x:30, text:"T1023"}, {x:40, text:"D/R"}, {x:50, text:"H0044"}, {x:60, text:"D/R"}]],

        [[{x:5, text:"1"}, {x:6, text:"Alvarado, Desiree"}, {x:21, text:"00006379999"}, {x:31, text:"0221F7P2K"}, {x:41, text:"2/20-5/20"}, {x:51, text:"0519MLRQG"}, {x:61, text:"05/20-8/20"}]],

        [[{x:5, text:"16"}, {x:6, text:"Villasenor, Casandra"}, {x:21, text:"0001115644"}, {x:31, text:"0219WM2M5"}, {x:41, text:"2/17-5/17"}, {x:51, text:"0528WGD6P"}, {x:61, text:"05/17-08/17"}]]

        Column positions ~5,20,30,40,50,60

        Labels: NAME, MRN#, T1023, D/R, H0044, D/R

        Map: Name=0, MemberID=1, T1023AuthId=2, T1023Range=3, H0044AuthId=4, H0044Range=5

        Row1: Name="Alvarado, Desiree", MemberID="00006379999", T1023AuthId="0221F7P2K", T1023Range="2/20-5/20", H0044AuthId="0519MLRQG", H0044Range="05/20-8/20", T1023BillDate=null, H0044BillDate=null, Paid=null

        Row2: Name="Villasenor, Casandra", MemberID="0001115644", T1023AuthId="0219WM2M5", T1023Range="2/17-5/17", H0044AuthId="0528WGD6P", H0044Range="05/17-08/17", T1023BillDate=null, H0044BillDate=null, Paid=null

        PAGE_ROWS:
        ${inputData}
      `;

      const completion = await openai.chat.completions.create({
        model: 'gpt-4o',
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
        if (!data.rows || !Array.isArray(data.rows)) {
          throw new Error('Invalid structure: missing "rows" array');
        }
      } catch (jsonError) {
        console.error(`Job ${jobId} - JSON parse error:`, jsonError.message, content);
        throw jsonError;
      }

      const rows = data.rows;

      // Validate and normalize
      const normalized = rows.map(row => {
        const parsed = UnifiedRowSchema.safeParse(row);
        return parsed.success ? parsed.data : {};
      }).filter(row => Object.values(row).some(val => val != null)); // Remove empty rows

      jobs.set(jobId, { status: 'completed', data: normalized });
      console.log(`Job ${jobId} - Completed with ${normalized.length} rows`);
    } catch (e) {
      console.error(`Job ${jobId} - Processing error:`, e);
      jobs.set(jobId, { status: 'error', error: e.message });
    } finally {
      // Cleanup job after 30min
      setTimeout(() => jobs.delete(jobId), 30 * 60 * 1000);
    }
  });

  res.json({ status: true, jobId, message: 'Processing started. Poll /status/' + jobId });
});

app.get('/status/:jobId', (req, res) => {
  const jobId = req.params.jobId;
  const job = jobs.get(jobId);
  if (!job) {
    return res.status(404).json({ error: `Job ${jobId} not found` });
  }
  res.json(job.status === 'completed' ? { status: true, data: job.data } :
    job.status === 'error' ? { status: 'error', error: job.error } :
      { status: 'pending' });
});

// Start server
const PORT = process.env.PORT || 8000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});