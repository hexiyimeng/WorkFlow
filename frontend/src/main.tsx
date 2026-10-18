import ReactDOM from 'react-dom/client'
import { ReactFlowProvider } from '@xyflow/react';
import { FlowProvider } from './context/FlowContext'
import App from './App'
import './index.css'
import './components/nodes/node.css'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <ReactFlowProvider>
    <FlowProvider>
      <App />
    </FlowProvider>
  </ReactFlowProvider>
)
