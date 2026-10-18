import Header from './components/layout/Header';
import PluginDiagnosticsBanner from './components/layout/PluginDiagnosticsBanner';
import Sidebar from './components/layout/Sidebar';
import BottomPanel from './components/layout/BottomPanel';
import FlowEditor from './components/flow/FlowEditor';
import ExecutionSettingsDrawer from './components/execution/ExecutionSettingsDrawer';
import RecoveryBrowserDialog from './components/execution/RecoveryBrowserDialog';

export default function App() {
  return (
    <div className="workspace w-screen h-screen flex flex-col overflow-hidden font-sans"
      style={{ backgroundColor: 'var(--color-bg-app)', color: 'var(--color-text-primary)' }}>
      <Header />
      <PluginDiagnosticsBanner />
      <div className="workspace-body">
        <Sidebar />
        <main className="workspace-main">
          <FlowEditor />
          <BottomPanel />
        </main>
      </div>
      <ExecutionSettingsDrawer />
      <RecoveryBrowserDialog />
    </div>
  );
}
