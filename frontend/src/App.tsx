import Header from './components/layout/Header';
import PluginDiagnosticsBanner from './components/layout/PluginDiagnosticsBanner';
import Sidebar from './components/layout/Sidebar';
import BottomPanel from './components/layout/BottomPanel';
import FlowEditor from './components/flow/FlowEditor';
import ExecutionConfigDialog from './components/execution/ExecutionConfigDialog';
import RecoveryBrowserDialog from './components/execution/RecoveryBrowserDialog';

export default function App() {
  return (
    <div className="w-screen h-screen flex flex-col overflow-hidden font-sans transition-colors duration-300"
      style={{ backgroundColor: 'var(--color-bg-app)', color: 'var(--color-text-primary)' }}>
      <Header />
      <PluginDiagnosticsBanner />
      <div className="flex-1 flex overflow-hidden relative">
        <Sidebar />
        <main className="flex-1 relative flex flex-col overflow-hidden">
          <FlowEditor />
          <BottomPanel />
        </main>
      </div>
      <ExecutionConfigDialog />
      <RecoveryBrowserDialog />
    </div>
  );
}
