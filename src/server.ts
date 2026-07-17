import app from './app';
import { TaskWorkerEngine } from './tasks/infra/task.worker';

const PORT = process.env.PORT || 5000;

// Bootstrap background async processing engine cluster out-of-band from HTTP lifecycle
new TaskWorkerEngine();

app.listen(PORT, () => {
  console.log(`[System Active] Enterprise Ingress Controller fully bound online on port: ${PORT}`);
});
