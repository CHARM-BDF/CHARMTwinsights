import type { ModelRunRecord } from '../lib/contracts/types';
import { JsonPreview } from './JsonPreview';
import styles from './RunResultPanel.module.css';

type Props = {
  run: ModelRunRecord;
};

export function RunResultPanel({ run }: Props) {
  return (
    <section className={styles.panel}>
      <h3>Run Result</h3>
      <p>{run.resultPreview}</p>
      <div className={styles.grid}>
        <JsonPreview title="Input Snapshot" value={run.inputSnapshot} />
        <JsonPreview title="Output Snapshot" value={run.outputSnapshot} />
      </div>
    </section>
  );
}
