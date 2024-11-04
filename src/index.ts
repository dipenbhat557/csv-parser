import { ChatOpenAI } from "@langchain/openai";
import { PromptTemplate } from "@langchain/core/prompts";
import { StringOutputParser } from "@langchain/core/output_parsers";
import { configDotenv } from "dotenv";
import * as fs from "fs";
import * as readline from "readline"; 
import path from "path";
import express from "express"

configDotenv();

const app = express()

const openAIApiKey = process.env.OPENAI_API_KEY || "";
const llm = new ChatOpenAI({ openAIApiKey, temperature: 0 });

const sanitizeTemplate = `
You are a data cleaning assistant. Your task is to extract and clean the question data from the following values.
Remove all HTML tags, CSS, and formatting while preserving the actual text content.

For the following data where column names and values are separated by " \\n ":
{sqlData}

Extract and clean the following fields using their respective column indices:
- ID (index 0)
- Test Series (index 1)
- Test Title (index 2)
- Test Section (index 3)
- Question text (index 6) - remove all HTML/CSS
- Options 1-10 (indices 9-18) - remove all HTML/CSS
- Answer (index 32)

Return ONLY a single line in this exact CSV format (with quotes around text fields):
id,test_series,test_title,test_section,question,option_1,option_2,option_3,option_4,option_5,option_6,option_7,option_8,option_9,option_10,answer

Do not add any explanations or additional text. Just return the CSV line.`;

const sanitizePrompt = PromptTemplate.fromTemplate(sanitizeTemplate);
const sanitizeChain = sanitizePrompt.pipe(llm).pipe(new StringOutputParser());

async function processBatch(batch:any, outputStream:any) {
  for (const entry of batch) {
    try {
      const sanitizedRow = await sanitizeChain.invoke({ sqlData: entry.trim() });
      await new Promise((resolve, reject) => {
        outputStream.write(`${sanitizedRow}\n`, (err:any) => {
          if (err) reject(err);
          else resolve(null);
        });
      });
    } catch (error) {
      console.error("Error processing row:", error);
    }
  }
}

async function convertSQLToCSV(sqlPath:string) {
  const outputPath = "./output.csv";
  const outputStream = fs.createWriteStream(outputPath, { flags: 'a' });

  const rl = readline.createInterface({
    input: fs.createReadStream(sqlPath),
    crlfDelay: Infinity,
  });

  const batchSize = 50; 
  let batch = [];
  let lineCount = 0;

  for await (const line of rl) {
    const trimmedLine = line.trim();
    if (trimmedLine) {
      batch.push(trimmedLine);
      lineCount++;
      
      if (lineCount % batchSize === 0) {
        await processBatch(batch, outputStream);
        batch = []; 
      }
    }
  }

  if (batch.length > 0) {
    await processBatch(batch, outputStream);
  }

  outputStream.end();
  console.log("Conversion completed successfully to output.csv");
}

const sqlPath = path.join(__dirname, "..", "appx_question_answer.sql")

app.listen(3000,()=>{
    console.log("converting sql to csv")
    convertSQLToCSV(sqlPath)
})