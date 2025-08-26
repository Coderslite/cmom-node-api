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

    console.log('Extracted text lines:', allTextLines); // Debug log
  } catch (e) {
    console.error('PDF parsing error:', e);
    return res.status(500).json({ status: false, data: [], error: `PDF read error: ${e.message}` });
  }

  if (!allTextLines.length) {
    console.log('No text lines extracted from PDF');
    return res.status(200).json({ status: false, data: [], error: 'No text extracted from PDF' });
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
      console.log('Stopped filtering at marker:', line);
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
    console.log('No lines passed filtering, using all text lines');
    filteredLines = allTextLines;
  }

  console.log('Filtered lines:', filteredLines); // Debug log

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

  try {
    const completion = await openai.chat.completions.create({
      model: 'gpt-4o-mini',
      messages: [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: userInstructions },
      ],
      response_format: { type: 'json_object' },
      temperature: 0,
      max_tokens: 4000, // Increased to prevent truncation
    });

    const content = completion.choices[0].message.content;
    console.log('OpenAI response:', content); // Debug log

    let data;
    try {
      data = JSON.parse(content);
    } catch (jsonError) {
      console.error('JSON parse error:', jsonError.message);
      console.error('Raw OpenAI response:', content);
      return res.status(500).json({
        status: false,
        data: [],
        error: `AI extraction failed: Invalid JSON response`,
        rawResponse: content,
      });
    }

    const rows = data.rows || [];

    // Validate and normalize rows
    const normalized = rows.map(row => {
      const parsed = UnifiedRowSchema.safeParse(row);
      return parsed.success ? parsed.data : {};
    });

    // If no rows, include filtered lines for debugging
    if (!normalized.length) {
      return res.json({
        status: true,
        data: [],
        filteredLines,
        error: 'No rows extracted',
      });
    }

    res.json({ status: true, data: normalized });
  } catch (e) {
    console.error('AI extraction error:', e);
    res.status(500).json({ status: false, data: [], error: `AI extraction failed: ${e.message}` });
  }
});

// Start server
const PORT = process.env.PORT || 8000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});