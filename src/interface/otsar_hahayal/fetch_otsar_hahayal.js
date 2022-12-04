import {CompanyTypes, createScraper} from 'israeli-bank-scrapers';
import * as dotenv from 'dotenv' // see https://github.com/motdotla/dotenv#how-do-i-use-dotenv-with-import
import { program } from 'commander';

(async function () {
    try {
        dotenv.config()
        program
            .option('-d, --date <char>')
            .option('-m, --months <int>')
            .option('-i, --username <char>')
            .option('-p, --password <char>');
        program.parse();
        const args = program.opts();

        // read documentation below for available options
        const options = {
            companyId: CompanyTypes.otsarHahayal,
            verbose: true,
            startDate: new Date(args.date),
            combineInstallments: false,
            showBrowser: true
        };


        // read documentation below for information about credentials
        const credentials = {
          username: args.username,
          password: args.password
        };

        const scraper = createScraper(options);
        const scrapeResult = await scraper.scrape(credentials);

        if (scrapeResult.success) {
            const buf = new Buffer.from(JSON.stringify(scrapeResult));
            process.stdout.write(buf);
        } else {
            throw new Error(scrapeResult.errorType);
        }
    } catch (e) {
        process.stderr.write(`scraping failed for the following reason: ${e.message}`)}
})();