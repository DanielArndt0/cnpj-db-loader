import { stdin as input, stdout as output } from "node:process";
import { createInterface } from "node:readline";

export async function confirm(question: string): Promise<boolean> {
  return await new Promise((resolve, reject) => {
    const rl = createInterface({ input, output });

    rl.question(`${question} [y/N] `, (answer) => {
      const normalized = answer.trim().toLowerCase();
      rl.close();
      resolve(normalized === "y" || normalized === "yes");
    });

    rl.on("error", (error) => {
      rl.close();
      reject(error);
    });
  });
}
