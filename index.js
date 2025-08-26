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
      // 1) Extract text per page
      let allTextLines = [];
      const pdfParser = new PDFParser();
      const pdfData = await new Promise((resolve, reject) => {
        pdfParser.on('pdfParser_dataError', errData => reject(errData));
        pdfParser.on('pdfParser_dataReady', pdfData => resolve(pdfData));
        pdfParser.parseBuffer(req.file.buffer);
      });

      allTextLines = pdfData.Pages
        .flatMap(page => {
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
          return Object.values(textsByRow)
            .map(row => row.sort((a, b) => a.x - b.x).map(item => item.text).join(' ').replace(/\s+/g, ' ').trim())
            .filter(line => line);
        });

      console.log(`Job ${jobId} - Extracted text lines:`, allTextLines);

      if (!allTextLines.length) {
        throw new Error('No text extracted from PDF');
      }

      // 2) Improved filtering: Include potential data lines, attempt to group if fragmented
      const stopMarkers = [
        "AP'S OVERDUE", "AP'S DUE", "OVERDUE AP", "DUE CM", "SEPTEMBER", "ALL INTAKE", "NEEDS H0044", "CM"
      ];
      let filteredLines = [];
      let currentRow = [];
      for (const line of allTextLines) {
        const upperLine = line.toUpperCase();
        if (stopMarkers.some(marker => upperLine.includes(marker))) {
          if (currentRow.length) filteredLines.push(currentRow.join(' ').trim());
          break;
        }
        if (/^\d+$/.test(line) || (currentRow.length === 0 && /^\d+\s/.test(line))) { // Start new row on number
          if (currentRow.length) filteredLines.push(currentRow.join(' ').trim());
          currentRow = [line];
        } else if (currentRow.length > 0) {
          currentRow.push(line); // Append to current row
        } else if (line.includes(',') || /\d{2}\/\d{2}/.test(line) || /[A-Z0-9]{5,}/.test(line)) {
          currentRow = [line]; // Start if seems data
        }
      }
      if (currentRow.length) filteredLines.push(currentRow.join(' ').trim());

      // Include header if not already
      const headerCandidates = allTextLines.filter(line => line.toUpperCase().includes('NAME') || line.toUpperCase().includes('T1023') || line.toUpperCase().includes('H0044'));
      if (headerCandidates.length) {
        filteredLines = [...headerCandidates, ...filteredLines];
      }

      if (!filteredLines.length) {
        filteredLines = allTextLines;
      }

      console.log(`Job ${jobId} - Filtered lines:`, filteredLines);

      // 3) Improved AI prompts with fragmented examples
      const systemPrompt = `
        You are a precise information extraction engine for billing tables from PDFs.
        The input is a list of lines, which may include headers, joined rows, or fragmented fields from rows.
        Tables vary by insurer but follow similar patterns: Name, MemberID (MRN/MBR), optional DOB, then T1023 auth/range/bill, then H0044 auth/range/bill, optional paid.
        Columns may shift if fields are missing.
        Group lines or parts into logical rows and map to the schema. Handle typos like "HOO44" as "H0044".
        Return ONLY valid JSON: {"rows": [ ... ]}. No extra text.
      `;

      const userInstructions = `
        Extract rows from these lines. The lines may be full row strings (fields joined with spaces) or individual fields (fragmented across lines).
        For fragmented, group consecutive fields into rows: rows start with optional number, then Name (often "Last, First"), MemberID, optional DOB, then auth IDs and ranges.

        - Identify Name: Contains comma or multiple capitals.
        - MemberID: Numeric/alphanumeric after name.
        - DOB: Date like "MM/DD/YYYY"; skip.
        - Auth IDs: Alphanumeric codes like "146080416" or "0227TPWN2".
        - Ranges: Date ranges like "4/1-6/30".
        - Assign first auth/range to T1023, second to H0044.
        - If only one pair after DOB or in H0044 position, assign to H0044.
        - BillDate: Usually missing or part of range.
        - Paid: Number like "200" at end.
        - Use null for missing.

        Examples (handle both joined and fragmented similarly by splitting if needed):

        Joined line: "1 Alo, Benjamin 9898293 146080416 4/1-6/30"
        -> {"Name": "Alo, Benjamin", "MemberID": "9898293", "T1023AuthId": "146080416", "T1023Range": "4/1-6/30", "H0044AuthId": null, "H0044Range": null, "Paid": null}

        Fragmented lines: ["1", "Alo, Benjamin", "9898293", "146080416", "4/1-6/30"]
        -> Same as above

        Joined line: "4 Carobrese, Pamela 14825101 145279520 12/1-2/28 146369822 5/1-7/31"
        -> {"Name": "Carobrese, Pamela", "MemberID": "14825101", "T1023AuthId": "145279520", "T1023Range": "12/1-2/28", "H0044AuthId": "146369822", "H0044Range": "5/1-7/31", "Paid": null}

        Fragmented lines: ["4", "Carobrese, Pamela", "14825101", "145279520", "12/1-2/28", "146369822", "5/1-7/31"]
        -> Same as above

        Joined line with DOB and only H0044: "7 Garner, Michelle 0002668068 11/10/1980 0829TUUVS 08/23-11/23/24"
        -> {"Name": "Garner, Michelle", "MemberID": "0002668068", "T1023AuthId": null, "T1023Range": null, "H0044AuthId": "0829TUUVS", "H0044Range": "08/23-11/23/24", "Paid": null}

        Fragmented lines: ["7", "Garner, Michelle", "0002668068", "11/10/1980", "0829TUUVS", "08/23-11/23/24"]
        -> Same as above

        Joined line with paid: "13 Lewis, Elizabeth 44701321 146858872 7/1-7/31 200"
        -> {"Name": "Lewis, Elizabeth", "MemberID": "44701321", "T1023AuthId": null, "T1023Range": null, "H0044AuthId": "146858872", "H0044Range": "7/1-7/31", "Paid": "200"}

        Schema fields: Name, MemberID, T1023AuthId, T1023Range, T1023BillDate, H0044AuthId, H0044Range, H0044BillDate, Paid

        LINES:
        ${JSON.stringify(filteredLines)}
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
      }).filter(row => Object.values(row).some(val => val)); // Remove empty rows

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