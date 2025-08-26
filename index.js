import express from 'express';
import multer from 'multer';
import PDFParser from 'pdf2json';
import { z } from 'zod';
import OpenAI from 'openai';
import dotenv from 'dotenv';

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
const upload = multer({ storage: multer.memoryStorage() });

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
    return res.status(400).json({ status: false, data: [], error: 'Please upload a PDF' });
  }

  // 1) Extract text per page
  let allTextLines = [];
  try {
    const pdfParser = new PDFParser();
    const pdfData = await new Promise((resolve, reject) => {
      pdfParser.on('pdfParser_dataError', errData => reject(errData));
      pdfParser.on('pdfParser_dataReady', pdfData => resolve(pdfData));
      pdfParser.parseBuffer(req.file.buffer);
    });

    // Extract text from pdf2json output
    allTextLines = pdfData.Pages
      .flatMap(page => page.Texts.map(text => decodeURIComponent(text.R[0].T)))
      .map(line => line.replace(/\s+/g, ' ').trim())
      .filter(line => line);
  } catch (e) {
    return res.status(500).json({ status: false, data: [], error: `PDF read error: ${e.message}` });
  }

  if (!allTextLines.length) {
    return res.status(200).json({ status: false, data: [] });
  }

  // 2) Heuristic: filter rows
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
      break;
    }
    if (
      line.toUpperCase().includes('NAME') &&
      (line.toUpperCase().includes('MRN') || line.toUpperCase().includes('MBR'))
    ) {
      filteredLines.push(line);
      continue;
    }
    if (/^\d+\s/.test(line) || (line.includes(',') && !line.toUpperCase().startsWith('AUGUST'))) {
      filteredLines.push(line);
    }
  }

  if (!filteredLines.length) {
    filteredLines = allTextLines;
  }

  // 3) Build AI prompt
  const systemPrompt = `
    You are a precise information extraction engine for billing tables.
    You will receive text lines from a PDF (header + rows).
    Return ONLY valid JSON of the form: {"rows": [ ... ]} with NO extra commentary.
  `;

  const userInstructions = `
We have billing tables from different insurers with slightly different headers.
Unify each row into this MERGED SCHEMA (use strings; use null if missing):

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
1) Pair RANGE/BILL columns correctly (T1023 vs H0044).
2) 'MemberID' comes from MRN#/MBR ID #.
3) Do not invent data. If a cell is blank, use null.
4) Keep date/range formats as found (e.g., '04/01-07/01').
5) Output strictly: {"rows": [ UnifiedRow, ... ]}.

LINES:
${JSON.stringify(filteredLines)}
`;

  try {
    const completion = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: userInstructions },
      ],
      response_format: { type: 'json_object' },
      temperature: 0,
    });

    const content = completion.choices[0].message.content;
    const data = JSON.parse(content);
    const rows = data.rows || [];

    // Validate and normalize rows
    const normalized = rows.map(row => {
      const parsed = UnifiedRowSchema.safeParse(row);
      return parsed.success ? parsed.data : {};
    });

    res.json({ status: true, data: normalized });
  } catch (e) {
    res.status(500).json({ status: false, data: [], error: `AI extraction failed: ${e.message}` });
  }
});

// Start server
const PORT = process.env.PORT || 8000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});