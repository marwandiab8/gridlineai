const legacy = require("./dailyPdfReportBuilderLegacy");
const { renderJournalPdf } = require("./journalPdfReportBuilder");

module.exports = {
  ...legacy,
  renderJournalPdf,
};
