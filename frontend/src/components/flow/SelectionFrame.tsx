import { useEffect } from 'react';
import { useStore, useStoreApi } from '@xyflow/react';
import './selection-frame.css';

/** Keep React Flow's temporary selection frame for both marquee and Ctrl-click
 * selection. Reuse its native group dragging; no extra graph node is created.
 */
export default function SelectionFrame() {
  const store = useStoreApi();
  const multiple = useStore(state => state.nodes.filter(node => node.selected && !node.hidden).length > 1);
  const active = useStore(state => state.nodesSelectionActive);
  const selecting = useStore(state => state.userSelectionActive);
  useEffect(() => {
    if (!selecting && active !== multiple) store.setState({ nodesSelectionActive: multiple });
  }, [store, multiple, active, selecting]);
  return null;
}
