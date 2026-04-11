import { useState, useCallback, useEffect, useRef } from 'react';
import Particles from "react-tsparticles";
import { loadSlim } from "@tsparticles/slim";
import { motion, AnimatePresence } from "framer-motion";
import {
    Loader2, CheckCircle, AlertCircle, Database, Search, UserCheck,
    Globe, Send, Play, Pause, LayoutDashboard, ArrowRight,
    ChevronRight, Zap, RefreshCw, Table, X, ExternalLink, Mail, Phone,
    Download, FileSpreadsheet, MapPin, Link2, Sparkles
} from "lucide-react";

// App version - increment this with each deployment to trigger update notification
const APP_VERSION = '2.3.0';

function App() {
    const [status, setStatus] = useState({
        step1: 'idle', // idle, loading, success, error
        step2: 'idle',
        step3: 'idle',
        step4: 'idle',
        step5: 'idle',
    });
    const [logs, setLogs] = useState([]);
    const [autopilot, setAutopilot] = useState(false);
    const [currentStep, setCurrentStep] = useState(0);
    const [stats, setStats] = useState({ leads: 0, domains: 0, owners: 0, enriched: 0, exported: 0 });
    const [ghlConfig, setGhlConfig] = useState({ webhookUrl: '', tag: 'lead_scraper' });
    const [leads, setLeads] = useState([]);
    const [showTable, setShowTable] = useState(true);
    const [isStreaming, setIsStreaming] = useState(false);
    const [streamConfig, setStreamConfig] = useState({
        limit: 50,
        findDomains: true,
        findOwners: true,
        enrichApify: false
    });
    const autopilotRef = useRef(autopilot);
    const logsEndRef = useRef(null);
    const eventSourceRef = useRef(null);
    const [showUpdatePopup, setShowUpdatePopup] = useState(false);

    // Check for app update on mount
    useEffect(() => {
        const lastVersion = localStorage.getItem('app_version');
        if (lastVersion && lastVersion !== APP_VERSION) {
            // New version detected - show update popup
            setShowUpdatePopup(true);
            // Auto-hide after 5 seconds
            setTimeout(() => setShowUpdatePopup(false), 5000);
        }
        // Store current version
        localStorage.setItem('app_version', APP_VERSION);
    }, []);

    // Keep ref in sync with state
    useEffect(() => {
        autopilotRef.current = autopilot;
    }, [autopilot]);

    // Auto-scroll logs
    useEffect(() => {
        logsEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    }, [logs]);

    // Load GHL settings on mount
    useEffect(() => {
        const loadGhlSettings = async () => {
            try {
                const res = await fetch('/api/ghl-settings', { credentials: 'include' });
                if (res.ok) {
                    const data = await res.json();
                    if (data.webhookUrl || data.tag) {
                        setGhlConfig({
                            webhookUrl: data.webhookUrl || '',
                            tag: data.tag || 'lead_scraper'
                        });
                    }
                }
            } catch (e) {
                console.log('Could not load GHL settings:', e);
            }
        };
        loadGhlSettings();
    }, []);

    const particlesInit = useCallback(async engine => {
        await loadSlim(engine);
    }, []);

    const addLog = (msg, type = 'info') => {
        const timestamp = new Date().toLocaleTimeString();
        const prefix = type === 'error' ? '❌' : type === 'success' ? '✅' : type === 'warning' ? '⚠️' : '📋';
        setLogs(prev => [...prev, { time: timestamp, msg, type, prefix }]);
    };

    const runStep = async (stepNum, stepName, endpoint, stepKey, body = {}, retries = 2) => {
        setStatus(prev => ({ ...prev, [stepKey]: 'loading' }));
        setCurrentStep(stepNum);
        addLog(`Starting ${stepName}...`);

        for (let attempt = 0; attempt <= retries; attempt++) {
            try {
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), 28000); // 28s timeout

                const res = await fetch(endpoint, {
                    method: 'POST',
                    credentials: 'include',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body),
                    signal: controller.signal
                });
                clearTimeout(timeoutId);

                // Check if response is HTML (error page) instead of JSON
                const contentType = res.headers.get('content-type');
                if (!contentType || !contentType.includes('application/json')) {
                    const text = await res.text();
                    if (text.trim().startsWith('<')) {
                        throw new Error('Server returned HTML instead of JSON - may need to re-authenticate');
                    }
                    throw new Error('Invalid response format');
                }

                const data = await res.json();

                if (res.ok) {
                    setStatus(prev => ({ ...prev, [stepKey]: 'success' }));
                    addLog(`${stepName} Completed: ${data.message || 'Success'}`, 'success');

                    // Update stats based on step
                    if (stepNum === 1) setStats(prev => ({ ...prev, leads: data.count || 0 }));
                    if (stepNum === 2) setStats(prev => ({ ...prev, domains: data.count || 0 }));
                    if (stepNum === 3) setStats(prev => ({ ...prev, owners: data.count || 0 }));
                    if (stepNum === 4) setStats(prev => ({ ...prev, enriched: data.count || 0 }));
                    if (stepNum === 5) setStats(prev => ({ ...prev, exported: data.count || 0 }));

                    return { success: true, data };
                } else {
                    throw new Error(data.error || 'Failed');
                }
            } catch (err) {
                console.error(`${stepName} attempt ${attempt + 1} failed:`, err);

                // If this was the last attempt, mark as error
                if (attempt === retries) {
                    setStatus(prev => ({ ...prev, [stepKey]: 'error' }));
                    addLog(`Error in ${stepName}: ${err.message}`, 'error');
                    return { success: false, error: err.message };
                }

                // Retry after a brief delay
                addLog(`${stepName} attempt ${attempt + 1} failed, retrying...`, 'warning');
                await new Promise(r => setTimeout(r, 1500));
            }
        }
    };

    // Real-time streaming fetch
    const startStreamingFetch = () => {
        if (isStreaming) {
            stopStreaming();
            return;
        }

        // Reset state
        setLeads([]);
        setStatus({ step1: 'loading', step2: 'idle', step3: 'idle', step4: 'idle', step5: 'idle' });
        setStats({ leads: 0, domains: 0, owners: 0, enriched: 0, exported: 0 });
        setLogs([]);
        setIsStreaming(true);
        setShowTable(true);

        addLog('🚀 Starting real-time fetch pipeline...', 'success');

        const params = new URLSearchParams({
            limit: streamConfig.limit,
            find_domains: streamConfig.findDomains,
            find_owners: streamConfig.findOwners,
            enrich_apify: streamConfig.enrichApify
        });

        const eventSource = new EventSource(`/api/fetch-leads-stream?${params}`);
        eventSourceRef.current = eventSource;

        eventSource.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);

                switch (data.type) {
                    case 'lead':
                        if (data.action === 'add') {
                            setLeads(prev => [...prev, data.lead]);
                            setStats(prev => ({ ...prev, leads: prev.leads + 1 }));
                        } else if (data.action === 'update') {
                            setLeads(prev => prev.map(lead =>
                                lead.id === data.id ? { ...lead, [data.field]: data.value } : lead
                            ));
                            if (data.field === 'domain') setStats(prev => ({ ...prev, domains: prev.domains + 1 }));
                            if (data.field === 'owner_name') setStats(prev => ({ ...prev, owners: prev.owners + 1 }));
                        } else if (data.action === 'update_multi') {
                            setLeads(prev => prev.map(lead =>
                                lead.id === data.id ? { ...lead, ...data.updates } : lead
                            ));
                            setStats(prev => ({ ...prev, enriched: prev.enriched + 1 }));
                        }
                        break;

                    case 'status':
                        addLog(data.message, 'info');
                        // Update step status based on step
                        if (data.step === 'scrape') setStatus(prev => ({ ...prev, step1: 'loading' }));
                        if (data.step === 'scrape_done') setStatus(prev => ({ ...prev, step1: 'success' }));
                        if (data.step === 'domains') setStatus(prev => ({ ...prev, step2: 'loading' }));
                        if (data.step === 'domains_done') setStatus(prev => ({ ...prev, step2: 'success' }));
                        if (data.step === 'owners') setStatus(prev => ({ ...prev, step3: 'loading' }));
                        if (data.step === 'owners_done') setStatus(prev => ({ ...prev, step3: 'success' }));
                        if (data.step === 'enrich') setStatus(prev => ({ ...prev, step4: 'loading' }));
                        if (data.step === 'enrich_done') setStatus(prev => ({ ...prev, step4: 'success' }));
                        if (data.step === 'save') setStatus(prev => ({ ...prev, step5: 'loading' }));
                        if (data.step === 'save_done') setStatus(prev => ({ ...prev, step5: 'success' }));
                        break;

                    case 'log':
                        addLog(data.message, data.level || 'info');
                        break;

                    case 'keepalive':
                        // Keepalive message - do nothing, just keeps connection alive
                        break;

                    case 'complete':
                        addLog(`🎉 Pipeline complete! ${data.saved} leads saved, ${data.duplicates} duplicates`, 'success');

                        // Log state breakdown if available
                        if (data.state_results && Object.keys(data.state_results).length > 0) {
                            addLog('-----------------------------------');
                            addLog('📊 Final State Breakdown:');
                            Object.entries(data.state_results)
                                .sort((a, b) => b[1] - a[1]) // Sort by count desc
                                .forEach(([state, count]) => {
                                    addLog(`${state}: ${count} leads scrapped`);
                                });
                            addLog('-----------------------------------');
                        }

                        setIsStreaming(false);
                        eventSource.close();
                        break;

                    case 'error':
                        addLog(`Error: ${data.message}`, 'error');
                        setIsStreaming(false);
                        eventSource.close();
                        break;
                }
            } catch (e) {
                console.error('Error parsing SSE data:', e);
            }
        };

        eventSource.onerror = (error) => {
            console.error('SSE Error:', error);
            addLog('Connection error - stream ended', 'warning');
            setIsStreaming(false);
            eventSource.close();
        };
    };

    const stopStreaming = () => {
        if (eventSourceRef.current) {
            eventSourceRef.current.close();
        }
        setIsStreaming(false);
        addLog('⏹️ Streaming stopped', 'warning');
    };

    // Export to CSV
    const exportToCSV = () => {
        if (leads.length === 0) {
            addLog('No data to export', 'warning');
            return;
        }

        const headers = ['Business Name', 'Business Category', 'State', 'Filing Date', 'Address', 'Domain', 'Owner First Name', 'Owner Last Name', 'Phone 1', 'Phone 2', 'Email 1', 'Email 2', 'Source', 'Source URL'];
        const csvData = leads.map(lead => [
            lead.business_name || '',
            lead.industry || lead.business_category || '',
            lead.state || '',
            lead.filing_date || '',
            (lead.address || '').replace(/,/g, ' '),
            lead.domain || '',
            lead.owner_first_name || (lead.owner_name ? lead.owner_name.split(' ')[0] : '') || '',
            lead.owner_last_name || (lead.owner_name ? lead.owner_name.split(' ').slice(1).join(' ') : '') || '',
            lead.owner_phone_1 || lead.phone || '',
            lead.owner_phone_2 || '',
            lead.owner_email_1 || lead.email || '',
            lead.owner_email_2 || '',
            lead.source || '',
            lead.source_url || lead.url || ''
        ]);

        const csvContent = [headers, ...csvData].map(row => row.map(cell => `"${cell}"`).join(',')).join('\n');
        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `leads_${new Date().toISOString().slice(0, 10)}.csv`;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
        addLog(`Exported ${leads.length} leads to CSV`, 'success');
    };

    // Export to Google Sheets
    const exportToGoogleSheets = async () => {
        if (leads.length === 0) {
            addLog('No data to export', 'warning');
            return;
        }

        addLog('Exporting to Google Sheets...', 'info');
        try {
            const res = await fetch('/api/export-sheets-stream', {
                method: 'POST',
                credentials: 'include',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ leads: leads })
            });
            const data = await res.json();
            if (res.ok && data.success) {
                addLog(`Exported ${data.count || leads.length} leads to Google Sheets`, 'success');
                if (data.sheet_url) {
                    window.open(data.sheet_url, '_blank');
                }
            } else {
                const errorMsg = data.error || 'Unknown error';
                addLog(`Export failed: ${errorMsg}`, 'error');

                // If it's a quota error, provide a more detailed UI feedback
                if (errorMsg.toLowerCase().includes('quota') || errorMsg.toLowerCase().includes('storage')) {
                    addLog('💡 TIP: Go to Settings and click "Connect to Google" to use your own account with full Drive storage.', 'info');
                    if (data.details) {
                        addLog(`Instruction: ${data.details}`, 'warning');
                    }
                }
            }
        } catch (e) {
            addLog(`Export error: ${e.message}`, 'error');
        }
    };

    const runAutopilot = async () => {
        setAutopilot(true);
        addLog('🚀 AUTOPILOT MODE ACTIVATED', 'success');

        // Reset all steps
        setStatus({ step1: 'idle', step2: 'idle', step3: 'idle', step4: 'idle', step5: 'idle' });
        setStats({ leads: 0, domains: 0, owners: 0, enriched: 0, exported: 0 });

        // Step 1: Fetch Leads - INCREASED limit for maximum leads
        const step1Result = await runStep(1, "Fetch Leads", "/api/fetch-leads", "step1", { limit: 200 });
        if (!step1Result.success || !autopilotRef.current) {
            setAutopilot(false);
            if (!autopilotRef.current) addLog('Autopilot stopped by user', 'warning');
            return;
        }
        await new Promise(r => setTimeout(r, 1500));

        // Step 2: Find Domains - with extended limit
        const step2Result = await runStep(2, "Find Domains", "/api/find-domains", "step2", { limit: 100 });
        if (!step2Result.success || !autopilotRef.current) {
            setAutopilot(false);
            if (!autopilotRef.current) addLog('Autopilot stopped by user', 'warning');
            return;
        }
        await new Promise(r => setTimeout(r, 1500));

        // Step 3: Find Owners - reduced limit for faster response
        const step3Result = await runStep(3, "Find Owners", "/api/fetch-owners", "step3", { limit: 40 });
        if (!step3Result.success || !autopilotRef.current) {
            setAutopilot(false);
            if (!autopilotRef.current) addLog('Autopilot stopped by user', 'warning');
            return;
        }
        await new Promise(r => setTimeout(r, 1500));

        // Step 4: Enrich Data - reduced limit
        const step4Result = await runStep(4, "Enrich Data", "/api/enrich-leads", "step4", { limit: 30 });
        if (!step4Result.success || !autopilotRef.current) {
            setAutopilot(false);
            if (!autopilotRef.current) addLog('Autopilot stopped by user', 'warning');
            return;
        }
        await new Promise(r => setTimeout(r, 1500));

        // Step 5: Export to GHL
        const step5Result = await runStep(5, "Export to GHL", "/api/export-ghl", "step5", ghlConfig);

        setAutopilot(false);
        if (step5Result.success) {
            addLog('🎉 AUTOPILOT COMPLETE - All steps finished successfully!', 'success');
        }
    };

    const stopAutopilot = () => {
        setAutopilot(false);
        addLog('⏹️ Autopilot stopped', 'warning');
    };

    const steps = [
        { num: 1, key: 'step1', title: "Fetch Leads", desc: "Real-time scrapers for all 50 US States + SEC", icon: Database, color: "sky", endpoint: "/api/fetch-leads" },
        { num: 2, key: 'step2', title: "Find Domains", desc: "Discover websites via Serper Google API", icon: Globe, color: "emerald", endpoint: "/api/find-domains" },
        { num: 3, key: 'step3', title: "Find Owners", desc: "8 techniques: WHOIS, LinkedIn, SEC, etc.", icon: Search, color: "indigo", endpoint: "/api/fetch-owners" },
        { num: 4, key: 'step4', title: "Enrich Data", desc: "Apify skip tracing for emails & phones", icon: UserCheck, color: "purple", endpoint: "/api/enrich-leads" },
        { num: 5, key: 'step5', title: "Export to GHL", desc: "Push contacts to GoHighLevel CRM", icon: Send, color: "pink", endpoint: "/api/export-ghl" },
    ];

    return (
        <div className="relative min-h-screen w-full overflow-hidden bg-slate-900 text-white font-sans">
            {/* Update Notification Popup */}
            <AnimatePresence>
                {showUpdatePopup && (
                    <motion.div
                        initial={{ opacity: 0, y: -100, scale: 0.8 }}
                        animate={{ opacity: 1, y: 0, scale: 1 }}
                        exit={{ opacity: 0, y: -50, scale: 0.8 }}
                        className="fixed top-6 left-1/2 transform -translate-x-1/2 z-50"
                    >
                        <div className="bg-gradient-to-r from-emerald-600 to-green-600 px-6 py-4 rounded-2xl shadow-2xl shadow-emerald-500/30 flex items-center gap-4 border border-emerald-400/30">
                            <div className="w-12 h-12 rounded-full bg-white/20 flex items-center justify-center">
                                <Sparkles className="w-6 h-6 text-white" />
                            </div>
                            <div>
                                <h3 className="text-lg font-bold text-white">System Updated Successfully!</h3>
                                <p className="text-emerald-100 text-sm">Version {APP_VERSION} is now live with new features</p>
                            </div>
                            <button
                                onClick={() => setShowUpdatePopup(false)}
                                className="ml-2 p-1 rounded-full hover:bg-white/20 transition-colors"
                            >
                                <X className="w-5 h-5 text-white" />
                            </button>
                        </div>
                    </motion.div>
                )}
            </AnimatePresence>

            <Particles
                id="tsparticles"
                init={particlesInit}
                options={{
                    background: { color: { value: "#0f172a" } },
                    fpsLimit: 120,
                    interactivity: {
                        events: { onClick: { enable: true, mode: "push" }, onHover: { enable: true, mode: "repulse" }, resize: true },
                        modes: { push: { quantity: 4 }, repulse: { distance: 200, duration: 0.4 } },
                    },
                    particles: {
                        color: { value: "#38bdf8" },
                        links: { color: "#38bdf8", distance: 150, enable: true, opacity: 0.5, width: 1 },
                        move: { direction: "none", enable: true, outModes: { default: "bounce" }, random: false, speed: 2, straight: false },
                        number: { density: { enable: true, area: 800 }, value: 80 },
                        opacity: { value: 0.5 },
                        shape: { type: "circle" },
                        size: { value: { min: 1, max: 3 } },
                    },
                    detectRetina: true,
                }}
                className="absolute inset-0 z-0"
            />

            <div className="relative z-10 container mx-auto px-4 py-8 min-h-screen">
                {/* Header */}
                {/* Header */}
                <div className="flex items-center justify-between mb-8">
                    <motion.div initial={{ opacity: 0, x: -50 }} animate={{ opacity: 1, x: 0 }}>
                        <h1 className="text-4xl font-bold text-transparent bg-clip-text bg-gradient-to-r from-sky-400 to-blue-600">
                            Fetch Leads
                        </h1>
                        <p className="text-slate-400 mt-1">Automated 5-stage lead generation pipeline</p>
                    </motion.div>

                    <motion.div initial={{ opacity: 0, x: 50 }} animate={{ opacity: 1, x: 0 }} className="flex gap-3">
                        <a
                            href="/dashboard"
                            className="px-4 py-2 rounded-lg bg-gradient-to-r from-purple-600 to-pink-600 hover:from-purple-500 hover:to-pink-500 text-white flex items-center gap-2 transition-all shadow-lg hover:shadow-purple-500/25"
                        >
                            <LayoutDashboard className="w-5 h-5" />
                            Back to Dashboard
                        </a>
                    </motion.div>
                </div>

                {/* Stats Bar */}
                <motion.div
                    initial={{ opacity: 0, y: -20 }}
                    animate={{ opacity: 1, y: 0 }}
                    className="grid grid-cols-5 gap-4 mb-8"
                >
                    {[
                        { label: 'Leads', value: stats.leads, color: 'sky' },
                        { label: 'Domains', value: stats.domains, color: 'emerald' },
                        { label: 'Owners', value: stats.owners, color: 'indigo' },
                        { label: 'Enriched', value: stats.enriched, color: 'purple' },
                        { label: 'Exported', value: stats.exported, color: 'pink' },
                    ].map((stat, i) => (
                        <div key={i} className={`bg-slate-800/50 backdrop-blur-sm rounded-xl border border-slate-700 p-4 text-center`}>
                            <div className={`text-2xl font-bold text-${stat.color}-400`}>{stat.value}</div>
                            <div className="text-sm text-slate-400">{stat.label}</div>
                        </div>
                    ))}
                </motion.div>

                {/* Autopilot Control */}
                <motion.div
                    initial={{ opacity: 0, scale: 0.95 }}
                    animate={{ opacity: 1, scale: 1 }}
                    className="mb-8 flex justify-center"
                >
                    <button
                        onClick={autopilot ? stopAutopilot : runAutopilot}
                        className={`px-8 py-4 rounded-2xl font-bold text-lg flex items-center gap-3 transition-all shadow-lg ${autopilot
                            ? 'bg-red-600 hover:bg-red-500 shadow-red-500/25'
                            : 'bg-gradient-to-r from-green-600 to-emerald-600 hover:from-green-500 hover:to-emerald-500 shadow-green-500/25'
                            }`}
                    >
                        {autopilot ? (
                            <>
                                <Pause className="w-6 h-6" />
                                Stop Autopilot
                            </>
                        ) : (
                            <>
                                <Zap className="w-6 h-6" />
                                Run Autopilot (All Steps)
                            </>
                        )}
                    </button>
                </motion.div>

                {/* Real-Time Streaming Section */}
                <motion.div
                    initial={{ opacity: 0, y: 20 }}
                    animate={{ opacity: 1, y: 0 }}
                    className="mb-8 bg-slate-800/50 backdrop-blur-sm rounded-2xl border border-slate-700 p-6"
                >
                    <div className="flex items-center justify-between mb-4">
                        <div className="flex items-center gap-3">
                            <Table className="w-6 h-6 text-cyan-400" />
                            <h2 className="text-lg font-bold text-white">Real-Time Data Table</h2>
                            {isStreaming && (
                                <span className="px-2 py-1 rounded-full bg-green-500/20 text-green-400 text-xs flex items-center gap-1">
                                    <RefreshCw className="w-3 h-3 animate-spin" />
                                    Live
                                </span>
                            )}
                        </div>
                        <div className="flex items-center gap-4">
                            <label className="flex items-center gap-2 text-sm text-slate-400">
                                <input
                                    type="checkbox"
                                    checked={streamConfig.findDomains}
                                    onChange={(e) => setStreamConfig(prev => ({ ...prev, findDomains: e.target.checked }))}
                                    className="rounded border-slate-600 bg-slate-700 text-cyan-500"
                                    disabled={isStreaming}
                                />
                                Domains
                            </label>
                            <label className="flex items-center gap-2 text-sm text-slate-400">
                                <input
                                    type="checkbox"
                                    checked={streamConfig.findOwners}
                                    onChange={(e) => setStreamConfig(prev => ({ ...prev, findOwners: e.target.checked }))}
                                    className="rounded border-slate-600 bg-slate-700 text-green-500"
                                    disabled={isStreaming}
                                />
                                Owners
                            </label>
                            <label className="flex items-center gap-2 text-sm text-slate-400">
                                <input
                                    type="checkbox"
                                    checked={streamConfig.enrichApify}
                                    onChange={(e) => setStreamConfig(prev => ({ ...prev, enrichApify: e.target.checked }))}
                                    className="rounded border-slate-600 bg-slate-700 text-purple-500"
                                    disabled={isStreaming}
                                />
                                Apify Enrich
                            </label>
                            <select
                                value={streamConfig.limit}
                                onChange={(e) => setStreamConfig(prev => ({ ...prev, limit: parseInt(e.target.value) }))}
                                className="px-3 py-1.5 rounded-lg bg-slate-700 border border-slate-600 text-white text-sm"
                                disabled={isStreaming}
                            >
                                <option value="20">20 leads</option>
                                <option value="50">50 leads</option>
                                <option value="75">75 leads</option>
                                <option value="100">100 leads</option>
                            </select>
                            <button
                                onClick={startStreamingFetch}
                                disabled={autopilot}
                                className={`px-6 py-2 rounded-xl font-bold text-sm flex items-center gap-2 transition-all shadow-lg ${isStreaming
                                    ? 'bg-red-600 hover:bg-red-500'
                                    : 'bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500'
                                    } disabled:opacity-50`}
                            >
                                {isStreaming ? (
                                    <>
                                        <X className="w-4 h-4" />
                                        Stop
                                    </>
                                ) : (
                                    <>
                                        <Play className="w-4 h-4" />
                                        Start Real-Time Fetch
                                    </>
                                )}
                            </button>
                        </div>
                    </div>

                    {/* Export Buttons */}
                    {leads.length > 0 && (
                        <div className="flex items-center gap-3 mb-4">
                            <span className="text-sm text-slate-400">Export {leads.length} leads:</span>
                            <button
                                onClick={exportToCSV}
                                className="px-4 py-2 rounded-lg bg-emerald-600 hover:bg-emerald-500 text-white text-sm font-medium flex items-center gap-2 transition-all"
                            >
                                <Download className="w-4 h-4" />
                                Download CSV
                            </button>
                            <button
                                onClick={exportToGoogleSheets}
                                className="px-4 py-2 rounded-lg bg-blue-600 hover:bg-blue-500 text-white text-sm font-medium flex items-center gap-2 transition-all"
                            >
                                <FileSpreadsheet className="w-4 h-4" />
                                Export to Google Sheets
                            </button>
                        </div>
                    )}

                    {/* Data Table */}
                    {showTable && (
                        <div className="overflow-x-auto rounded-xl border border-slate-700 max-h-[500px] overflow-y-auto" style={{ minWidth: '100%' }}>
                            <table className="min-w-[1800px] text-sm">
                                <thead className="bg-slate-900/50 sticky top-0 z-10">
                                    <tr>
                                        <th className="px-3 py-2 text-left text-xs text-slate-400 font-medium whitespace-nowrap">#</th>
                                        <th className="px-3 py-2 text-left text-xs text-slate-400 font-medium whitespace-nowrap">Business Name</th>
                                        <th className="px-3 py-2 text-left text-xs text-slate-400 font-medium whitespace-nowrap">Category</th>
                                        <th className="px-3 py-2 text-left text-xs text-slate-400 font-medium whitespace-nowrap">State</th>
                                        <th className="px-3 py-2 text-left text-xs text-slate-400 font-medium whitespace-nowrap">Filing Date</th>
                                        <th className="px-3 py-2 text-left text-xs text-slate-400 font-medium whitespace-nowrap">Address</th>
                                        <th className="px-3 py-2 text-left text-xs text-slate-400 font-medium whitespace-nowrap">Domain</th>
                                        <th className="px-3 py-2 text-left text-xs text-slate-400 font-medium whitespace-nowrap">Owner First</th>
                                        <th className="px-3 py-2 text-left text-xs text-slate-400 font-medium whitespace-nowrap">Owner Last</th>
                                        <th className="px-3 py-2 text-left text-xs text-slate-400 font-medium whitespace-nowrap">Phone 1</th>
                                        <th className="px-3 py-2 text-left text-xs text-slate-400 font-medium whitespace-nowrap">Phone 2</th>
                                        <th className="px-3 py-2 text-left text-xs text-slate-400 font-medium whitespace-nowrap">Email 1</th>
                                        <th className="px-3 py-2 text-left text-xs text-slate-400 font-medium whitespace-nowrap">Email 2</th>
                                        <th className="px-3 py-2 text-left text-xs text-slate-400 font-medium whitespace-nowrap">Source</th>
                                        <th className="px-3 py-2 text-left text-xs text-slate-400 font-medium whitespace-nowrap">Source URL</th>
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-slate-700/50">
                                    {leads.length === 0 ? (
                                        <tr>
                                            <td colSpan="15" className="px-4 py-8 text-center text-slate-500">
                                                {isStreaming ? (
                                                    <span className="flex items-center justify-center gap-2">
                                                        <Loader2 className="w-5 h-5 animate-spin" />
                                                        Waiting for data...
                                                    </span>
                                                ) : (
                                                    'Click "Start Real-Time Fetch" to begin scraping leads'
                                                )}
                                            </td>
                                        </tr>
                                    ) : (
                                        leads.map((lead, idx) => (
                                            <motion.tr
                                                key={lead.id}
                                                initial={{ opacity: 0, backgroundColor: 'rgba(56, 189, 248, 0.2)' }}
                                                animate={{ opacity: 1, backgroundColor: 'transparent' }}
                                                transition={{ duration: 0.5 }}
                                                className="hover:bg-slate-800/30"
                                            >
                                                <td className="px-3 py-2 text-slate-500 whitespace-nowrap">{idx + 1}</td>
                                                <td className="px-3 py-2 whitespace-nowrap">
                                                    <div className="font-medium text-white max-w-[200px] truncate" title={lead.business_name}>
                                                        {lead.business_name}
                                                    </div>
                                                </td>
                                                <td className="px-3 py-2 whitespace-nowrap">
                                                    {lead.industry || lead.business_category ? (
                                                        <span className="px-2 py-0.5 rounded text-xs bg-orange-500/20 text-orange-400 max-w-[120px] truncate inline-block" title={lead.industry || lead.business_category}>
                                                            {(lead.industry || lead.business_category || '').substring(0, 20)}{(lead.industry || lead.business_category || '').length > 20 ? '...' : ''}
                                                        </span>
                                                    ) : (
                                                        <span className="text-slate-600">-</span>
                                                    )}
                                                </td>
                                                <td className="px-3 py-2 whitespace-nowrap">
                                                    <span className="px-2 py-0.5 rounded text-xs bg-slate-700 text-slate-300">
                                                        {lead.state}
                                                    </span>
                                                </td>
                                                <td className="px-3 py-2 text-slate-300 whitespace-nowrap">
                                                    {lead.filing_date || <span className="text-slate-600">-</span>}
                                                </td>
                                                <td className="px-3 py-2 whitespace-nowrap">
                                                    <div className="flex items-center gap-1 text-slate-300 max-w-[200px] truncate" title={lead.address}>
                                                        <MapPin className="w-3 h-3 text-slate-500 flex-shrink-0" />
                                                        {lead.address || <span className="text-slate-600">-</span>}
                                                    </div>
                                                </td>
                                                <td className="px-3 py-2 whitespace-nowrap">
                                                    {lead.domain ? (
                                                        <a
                                                            href={`https://${lead.domain}`}
                                                            target="_blank"
                                                            rel="noopener noreferrer"
                                                            className="text-cyan-400 hover:text-cyan-300 flex items-center gap-1"
                                                        >
                                                            {lead.domain.substring(0, 25)}
                                                            <ExternalLink className="w-3 h-3" />
                                                        </a>
                                                    ) : (
                                                        <span className="text-slate-600">-</span>
                                                    )}
                                                </td>
                                                <td className="px-3 py-2 text-slate-300 whitespace-nowrap">
                                                    {lead.owner_first_name || (lead.owner_name ? lead.owner_name.split(' ')[0] : null) || <span className="text-slate-600">-</span>}
                                                </td>
                                                <td className="px-3 py-2 text-slate-300 whitespace-nowrap">
                                                    {lead.owner_last_name || (lead.owner_name ? lead.owner_name.split(' ').slice(1).join(' ') : null) || <span className="text-slate-600">-</span>}
                                                </td>
                                                <td className="px-3 py-2 whitespace-nowrap">
                                                    {(lead.owner_phone_1 || lead.phone) ? (
                                                        <span className="text-green-400 flex items-center gap-1">
                                                            <Phone className="w-3 h-3" />
                                                            {lead.owner_phone_1 || lead.phone}
                                                        </span>
                                                    ) : (
                                                        <span className="text-slate-600">-</span>
                                                    )}
                                                </td>
                                                <td className="px-3 py-2 whitespace-nowrap">
                                                    {lead.owner_phone_2 ? (
                                                        <span className="text-green-400 flex items-center gap-1">
                                                            <Phone className="w-3 h-3" />
                                                            {lead.owner_phone_2}
                                                        </span>
                                                    ) : (
                                                        <span className="text-slate-600">-</span>
                                                    )}
                                                </td>
                                                <td className="px-3 py-2 whitespace-nowrap">
                                                    {(lead.owner_email_1 || lead.email) ? (
                                                        <a href={`mailto:${lead.owner_email_1 || lead.email}`} className="text-purple-400 hover:text-purple-300 flex items-center gap-1">
                                                            <Mail className="w-3 h-3" />
                                                            <span className="max-w-[150px] truncate">{lead.owner_email_1 || lead.email}</span>
                                                        </a>
                                                    ) : (
                                                        <span className="text-slate-600">-</span>
                                                    )}
                                                </td>
                                                <td className="px-3 py-2 whitespace-nowrap">
                                                    {lead.owner_email_2 ? (
                                                        <a href={`mailto:${lead.owner_email_2}`} className="text-purple-400 hover:text-purple-300 flex items-center gap-1">
                                                            <Mail className="w-3 h-3" />
                                                            <span className="max-w-[150px] truncate">{lead.owner_email_2}</span>
                                                        </a>
                                                    ) : (
                                                        <span className="text-slate-600">-</span>
                                                    )}
                                                </td>
                                                <td className="px-3 py-2 whitespace-nowrap">
                                                    <span className={`px-2 py-0.5 rounded text-xs ${lead.source?.includes('SEC') ? 'bg-blue-500/20 text-blue-400' :
                                                        lead.source?.includes('State') ? 'bg-green-500/20 text-green-400' :
                                                            'bg-purple-500/20 text-purple-400'
                                                        }`}>
                                                        {lead.source}
                                                    </span>
                                                </td>
                                                <td className="px-3 py-2 whitespace-nowrap">
                                                    {lead.source_url ? (
                                                        <a
                                                            href={lead.source_url}
                                                            target="_blank"
                                                            rel="noopener noreferrer"
                                                            className="text-amber-400 hover:text-amber-300 flex items-center gap-1"
                                                            title={lead.source_url}
                                                        >
                                                            <Link2 className="w-3 h-3" />
                                                            View Filing
                                                        </a>
                                                    ) : (
                                                        <span className="text-slate-600">-</span>
                                                    )}
                                                </td>
                                            </motion.tr>
                                        ))
                                    )}
                                </tbody>
                            </table>
                        </div>
                    )}
                </motion.div>

                {/* Pipeline Flow */}
                <div className="mb-8">
                    <div className="flex items-center justify-center gap-2 flex-wrap">
                        {steps.map((step, i) => (
                            <div key={step.num} className="flex items-center">
                                <StepCard
                                    {...step}
                                    status={status[step.key]}
                                    isActive={currentStep === step.num}
                                    onClick={() => !autopilot && runStep(step.num, step.title, step.endpoint, step.key, step.num === 5 ? ghlConfig : {})}
                                    disabled={autopilot || (step.num > 1 && status[`step${step.num - 1}`] !== 'success')}
                                />
                                {i < steps.length - 1 && (
                                    <div className="hidden md:flex items-center mx-2">
                                        <motion.div
                                            animate={{
                                                opacity: status[step.key] === 'success' ? 1 : 0.3,
                                                scale: status[step.key] === 'success' ? [1, 1.2, 1] : 1
                                            }}
                                            transition={{ duration: 0.5, repeat: status[step.key] === 'loading' ? Infinity : 0 }}
                                        >
                                            <ChevronRight className={`w-6 h-6 ${status[step.key] === 'success' ? 'text-green-400' : 'text-slate-600'}`} />
                                        </motion.div>
                                    </div>
                                )}
                            </div>
                        ))}
                    </div>
                </div>

                {/* Progress Line */}
                <div className="mb-8 px-4 hidden md:block">
                    <div className="relative h-2 bg-slate-700 rounded-full overflow-hidden">
                        <motion.div
                            className="absolute left-0 top-0 h-full bg-gradient-to-r from-sky-500 via-purple-500 to-pink-500"
                            initial={{ width: '0%' }}
                            animate={{
                                width: `${Object.values(status).filter(s => s === 'success').length * 20}%`
                            }}
                            transition={{ duration: 0.5 }}
                        />
                    </div>
                </div>

                {/* Review Data Button - Shows when all steps complete */}
                <AnimatePresence>
                    {Object.values(status).every(s => s === 'success') && (
                        <motion.div
                            initial={{ opacity: 0, scale: 0.9 }}
                            animate={{ opacity: 1, scale: 1 }}
                            exit={{ opacity: 0, scale: 0.9 }}
                            className="mb-8 flex justify-center"
                        >
                            <a
                                href="/leads"
                                className="px-8 py-4 rounded-2xl font-bold text-lg flex items-center gap-3 transition-all shadow-lg bg-gradient-to-r from-pink-600 via-purple-600 to-indigo-600 hover:from-pink-500 hover:via-purple-500 hover:to-indigo-500 text-white shadow-purple-500/30 hover:shadow-purple-500/50 hover:scale-105"
                            >
                                <LayoutDashboard className="w-6 h-6" />
                                Review Scraped Data
                                <ArrowRight className="w-5 h-5" />
                            </a>
                        </motion.div>
                    )}
                </AnimatePresence>

                {/* Console Log */}
                <motion.div
                    initial={{ opacity: 0, y: 50 }}
                    animate={{ opacity: 1, y: 0 }}
                    className="w-full max-w-5xl mx-auto bg-black/50 backdrop-blur-md rounded-xl border border-slate-700 p-6 h-64 overflow-y-auto font-mono text-sm"
                >
                    <div className="flex items-center gap-2 mb-4 border-b border-slate-700 pb-2">
                        <div className="w-3 h-3 rounded-full bg-red-500"></div>
                        <div className="w-3 h-3 rounded-full bg-yellow-500"></div>
                        <div className="w-3 h-3 rounded-full bg-green-500"></div>
                        <span className="ml-2 text-slate-400">System Logs</span>
                        {autopilot && <span className="ml-auto text-green-400 flex items-center gap-2"><RefreshCw className="w-4 h-4 animate-spin" /> Autopilot Active</span>}
                    </div>
                    <div className="space-y-1">
                        {logs.length === 0 && <span className="text-slate-600">Ready to start... Click "Run Autopilot" or run steps individually.</span>}
                        {logs.map((log, i) => (
                            <div key={i} className={`${log.type === 'error' ? 'text-red-400' : log.type === 'success' ? 'text-green-400' : log.type === 'warning' ? 'text-yellow-400' : 'text-slate-300'}`}>
                                <span className="text-slate-500">[{log.time}]</span> {log.prefix} {log.msg}
                            </div>
                        ))}
                        <div ref={logsEndRef} />
                    </div>
                </motion.div>
            </div>
        </div>
    );
}

function StepCard({ num, title, desc, icon: Icon, color, status, isActive, onClick, disabled }) {
    const getStatusStyle = () => {
        if (status === 'loading') return 'border-blue-500 shadow-blue-500/30 shadow-lg';
        if (status === 'success') return 'border-green-500 shadow-green-500/30 shadow-lg';
        if (status === 'error') return 'border-red-500 shadow-red-500/30 shadow-lg';
        if (isActive) return `border-${color}-500/50`;
        return 'border-slate-700 hover:border-slate-500';
    };

    const colorMap = {
        sky: 'text-sky-400',
        emerald: 'text-emerald-400',
        indigo: 'text-indigo-400',
        purple: 'text-purple-400',
        pink: 'text-pink-400'
    };

    return (
        <motion.div
            whileHover={!disabled ? { scale: 1.02, y: -2 } : {}}
            whileTap={!disabled ? { scale: 0.98 } : {}}
            className={`relative p-5 rounded-2xl bg-slate-800/50 backdrop-blur-sm border-2 transition-all duration-300 w-44 ${getStatusStyle()} ${disabled ? 'opacity-50 cursor-not-allowed' : 'cursor-pointer'}`}
            onClick={!disabled && status !== 'loading' ? onClick : undefined}
        >
            {/* Step Number Badge */}
            <div className={`absolute -top-2 -left-2 w-7 h-7 rounded-full bg-${color}-500 flex items-center justify-center text-sm font-bold text-white shadow-lg`}>
                {num}
            </div>

            <div className="flex items-start justify-between mb-3">
                <Icon className={`w-7 h-7 ${colorMap[color]}`} />
                {status === 'loading' && <Loader2 className="w-5 h-5 animate-spin text-blue-400" />}
                {status === 'success' && <CheckCircle className="w-5 h-5 text-green-400" />}
                {status === 'error' && <AlertCircle className="w-5 h-5 text-red-400" />}
            </div>

            <h3 className="text-base font-bold mb-1 text-white">{title}</h3>
            <p className="text-slate-400 text-xs mb-3 line-clamp-2">{desc}</p>

            <button
                disabled={disabled || status === 'loading'}
                className={`w-full py-2 px-3 rounded-lg font-medium text-sm transition-colors ${status === 'success' ? 'bg-green-500/20 text-green-400 border border-green-500/30' :
                    status === 'error' ? 'bg-red-500/20 text-red-400 border border-red-500/30' :
                        `bg-${color}-600 hover:bg-${color}-500 text-white`
                    } disabled:bg-slate-700 disabled:text-slate-500`}
            >
                {status === 'loading' ? 'Running...' : status === 'success' ? 'Done ✓' : status === 'error' ? 'Retry' : 'Run'}
            </button>
        </motion.div>
    );
}

export default App;
