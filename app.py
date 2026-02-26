"""
Lead Generation Dashboard - Main Application
=============================================
A Streamlit-based web application for tracking newly registered businesses.

Features:
- Fetch new business registrations (mock or real data)
- View and filter leads in a data table
- Export leads to Google Sheets
- Local SQLite storage

To run:
    streamlit run app.py
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os

# Import application modules
from scraper_manager import ScraperManager
from database import Database, get_database
from google_sheets import GoogleSheetsExporter, MockGoogleSheetsExporter
from scrapers.mock_scraper import MockScraper


# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="Lead Generation Dashboard",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1f2937;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1rem;
        color: #6b7280;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f9fafb;
        border-radius: 0.5rem;
        padding: 1rem;
        border: 1px solid #e5e7eb;
    }
    .stButton > button {
        width: 100%;
    }
    .success-message {
        padding: 1rem;
        background-color: #d1fae5;
        border-radius: 0.5rem;
        color: #065f46;
    }
    .warning-message {
        padding: 1rem;
        background-color: #fef3c7;
        border-radius: 0.5rem;
        color: #92400e;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================================
# SESSION STATE INITIALIZATION
# ============================================================================

def init_session_state():
    """Initialize session state variables."""
    if "leads_df" not in st.session_state:
        st.session_state.leads_df = pd.DataFrame()
    
    if "last_fetch_time" not in st.session_state:
        st.session_state.last_fetch_time = None
    
    if "fetch_stats" not in st.session_state:
        st.session_state.fetch_stats = {}
    
    if "export_history" not in st.session_state:
        st.session_state.export_history = []


# ============================================================================
# CACHED RESOURCES
# ============================================================================

@st.cache_resource
def get_scraper_manager():
    """Get singleton scraper manager instance."""
    return ScraperManager(use_mock_fallback=True)


@st.cache_resource
def get_db():
    """Get singleton database instance."""
    return get_database("leads.db")


def get_sheets_exporter():
    """Get Google Sheets exporter (not cached due to authentication)."""
    exporter = GoogleSheetsExporter()
    if not exporter.is_configured():
        return MockGoogleSheetsExporter()
    return exporter


# ============================================================================
# MAIN COMPONENTS
# ============================================================================

def render_header():
    """Render the main header section."""
    st.markdown('<p class="main-header">🏢 Lead Generation Dashboard</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Track newly registered businesses across US states</p>', unsafe_allow_html=True)


def render_sidebar():
    """Render the sidebar with configuration options."""
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Fetch Settings
        st.subheader("Fetch Settings")
        
        limit_per_state = st.slider(
            "Records per state",
            min_value=5,
            max_value=100,
            value=20,
            step=5,
            help="Maximum number of business records to fetch per state"
        )
        
        # State Selection
        available_states = MockScraper.get_supported_states()
        selected_states = st.multiselect(
            "Select states",
            options=available_states,
            default=available_states,
            help="Choose which states to fetch data from"
        )
        
        st.divider()
        
        # Google Sheets Configuration
        st.subheader("Google Sheets Export")
        
        spreadsheet_id = st.text_input(
            "Spreadsheet ID",
            placeholder="Enter your Google Sheet ID",
            help="Found in the spreadsheet URL: docs.google.com/spreadsheets/d/{ID}/..."
        )
        
        exporter = get_sheets_exporter()
        if isinstance(exporter, MockGoogleSheetsExporter):
            st.warning("⚠️ Google Sheets not configured. Using mock export.")
            st.caption("Add `service_account.json` to enable real exports.")
        else:
            email = exporter.get_service_account_email()
            if email:
                st.info(f"📧 Share your sheet with:\n`{email}`")
        
        st.divider()
        
        # Database Stats
        st.subheader("📊 Database Stats")
        db = get_db()
        stats = db.get_stats()
        
        st.metric("Total Leads Stored", stats.get("total_leads", 0))
        
        if stats.get("leads_by_state"):
            with st.expander("Leads by State"):
                for state, count in stats["leads_by_state"].items():
                    st.write(f"• {state}: {count}")
        
        # Clear database button
        if st.button("🗑️ Clear Database", use_container_width=True):
            count = db.clear_all_leads()
            st.success(f"Cleared {count} leads from database")
            st.rerun()
        
        return {
            "limit_per_state": limit_per_state,
            "selected_states": selected_states,
            "spreadsheet_id": spreadsheet_id
        }


def fetch_data(states: list, limit: int):
    """Fetch business data from selected states."""
    manager = get_scraper_manager()
    db = get_db()
    
    with st.spinner("🔄 Fetching business data..."):
        # Fetch from scrapers
        records = manager.fetch_all(
            limit_per_state=limit,
            states=states if states else None,
            parallel=True
        )
        
        # Save to database
        if records:
            inserted, duplicates = db.save_records(records)
            
            # Convert to DataFrame for display
            records_data = [r.to_dict() for r in records]
            df = pd.DataFrame(records_data)
            
            # Store in session state
            st.session_state.leads_df = df
            st.session_state.last_fetch_time = datetime.now()
            st.session_state.fetch_stats = manager.get_last_run_stats()
            
            return True, len(records), inserted, duplicates
    
    return False, 0, 0, 0


def render_data_table(config: dict):
    """Render the main data table section."""
    st.header("📋 Business Leads")
    
    # Action buttons row
    col1, col2, col3, col4 = st.columns([2, 2, 2, 4])
    
    with col1:
        fetch_clicked = st.button(
            "🔄 Fetch Data",
            type="primary",
            use_container_width=True,
            help="Fetch new business registrations"
        )
    
    with col2:
        export_clicked = st.button(
            "📤 Export to Sheets",
            use_container_width=True,
            disabled=st.session_state.leads_df.empty,
            help="Export displayed data to Google Sheets"
        )
    
    with col3:
        load_from_db = st.button(
            "📁 Load from Database",
            use_container_width=True,
            help="Load previously saved leads from database"
        )
    
    with col4:
        if st.session_state.last_fetch_time:
            st.caption(f"Last fetch: {st.session_state.last_fetch_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Handle fetch action
    if fetch_clicked:
        success, total, inserted, duplicates = fetch_data(
            config["selected_states"],
            config["limit_per_state"]
        )
        
        if success:
            st.success(f"✅ Fetched {total} leads ({inserted} new, {duplicates} duplicates)")
        else:
            st.warning("No data fetched. Try selecting different states.")
    
    # Handle load from database
    if load_from_db:
        db = get_db()
        df = db.get_all_leads()
        if not df.empty:
            st.session_state.leads_df = df
            st.success(f"Loaded {len(df)} leads from database")
        else:
            st.warning("No leads found in database. Fetch some data first!")
    
    # Handle export action
    if export_clicked and not st.session_state.leads_df.empty:
        if config["spreadsheet_id"]:
            exporter = get_sheets_exporter()
            
            with st.spinner("📤 Exporting to Google Sheets..."):
                result = exporter.export_dataframe(
                    st.session_state.leads_df,
                    config["spreadsheet_id"]
                )
            
            if result.get("success"):
                st.success(f"✅ Exported {result['rows_exported']} rows to Google Sheets")
                if result.get("mock"):
                    st.info("(Mock export - configure Google Sheets for real export)")
            else:
                st.error(f"Export failed: {result.get('error', 'Unknown error')}")
        else:
            st.warning("Please enter a Spreadsheet ID in the sidebar")
    
    # Display data table
    if not st.session_state.leads_df.empty:
        df = st.session_state.leads_df.copy()
        
        # Filters row
        st.subheader("🔍 Filters")
        filter_col1, filter_col2, filter_col3 = st.columns(3)
        
        with filter_col1:
            search_term = st.text_input(
                "Search business name",
                placeholder="Type to search..."
            )
        
        with filter_col2:
            if "state" in df.columns:
                states_in_data = df["state"].unique().tolist()
                filter_states = st.multiselect(
                    "Filter by state",
                    options=states_in_data,
                    default=[]
                )
        
        with filter_col3:
            if "status" in df.columns:
                statuses = df["status"].unique().tolist()
                filter_status = st.multiselect(
                    "Filter by status",
                    options=statuses,
                    default=[]
                )
        
        # Apply filters
        filtered_df = df.copy()
        
        if search_term:
            filtered_df = filtered_df[
                filtered_df["business_name"].str.contains(search_term, case=False, na=False)
            ]
        
        if filter_states:
            filtered_df = filtered_df[filtered_df["state"].isin(filter_states)]
        
        if filter_status:
            filtered_df = filtered_df[filtered_df["status"].isin(filter_status)]
        
        # Display metrics
        st.divider()
        met_col1, met_col2, met_col3, met_col4 = st.columns(4)
        
        with met_col1:
            st.metric("Total Leads", len(filtered_df))
        
        with met_col2:
            if "state" in filtered_df.columns:
                st.metric("States", filtered_df["state"].nunique())
        
        with met_col3:
            if "filing_date" in filtered_df.columns and not filtered_df.empty:
                latest = filtered_df["filing_date"].max()
                st.metric("Latest Filing", latest)
        
        with met_col4:
            if "status" in filtered_df.columns:
                active = len(filtered_df[filtered_df["status"].str.contains("Active", case=False, na=False)])
                st.metric("Active Businesses", active)
        
        # Data table
        st.divider()
        
        # Select display columns
        display_columns = ["business_name", "filing_date", "state", "status", "url"]
        available_cols = [col for col in display_columns if col in filtered_df.columns]
        
        # Add optional columns if they exist and have data
        optional_cols = ["entity_type", "filing_number"]
        for col in optional_cols:
            if col in filtered_df.columns and filtered_df[col].notna().any():
                available_cols.append(col)
        
        display_df = filtered_df[available_cols].copy()
        
        # Configure column display
        column_config = {
            "business_name": st.column_config.TextColumn(
                "Business Name",
                width="large"
            ),
            "filing_date": st.column_config.DateColumn(
                "Filing Date",
                format="YYYY-MM-DD"
            ),
            "state": st.column_config.TextColumn(
                "State",
                width="small"
            ),
            "status": st.column_config.TextColumn(
                "Status",
                width="medium"
            ),
            "url": st.column_config.LinkColumn(
                "Details",
                display_text="🔗 View"
            ),
            "entity_type": st.column_config.TextColumn(
                "Entity Type",
                width="small"
            ),
            "filing_number": st.column_config.TextColumn(
                "Filing #",
                width="small"
            )
        }
        
        st.dataframe(
            display_df,
            column_config=column_config,
            use_container_width=True,
            hide_index=True,
            height=500
        )
        
        # Download button
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="📥 Download as CSV",
            data=csv,
            file_name=f"leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
        
    else:
        st.info("👆 Click 'Fetch Data' to get new business registrations, or 'Load from Database' to view saved leads.")


def render_stats_section():
    """Render the statistics section."""
    if st.session_state.fetch_stats:
        stats = st.session_state.fetch_stats
        
        with st.expander("📈 Last Fetch Statistics", expanded=False):
            stats_col1, stats_col2, stats_col3 = st.columns(3)
            
            with stats_col1:
                st.metric("Total Records", stats.get("total_records", 0))
            
            with stats_col2:
                st.metric("Successful States", stats.get("successful_states", 0))
            
            with stats_col3:
                st.metric("Execution Time", f"{stats.get('execution_time', 0):.2f}s")
            
            # Show per-state results
            if "results" in stats:
                st.subheader("Results by State")
                for result in stats["results"]:
                    status = "✅" if result.success else "❌"
                    st.write(
                        f"{status} **{result.state_name}**: "
                        f"{len(result.records)} records ({result.execution_time:.2f}s)"
                    )
                    if result.error_message:
                        st.caption(f"Error: {result.error_message}")


def render_footer():
    """Render the footer section."""
    st.divider()
    st.markdown("""
    <div style="text-align: center; color: #6b7280; font-size: 0.875rem;">
        <p>Lead Generation Dashboard | Built with Streamlit</p>
        <p>Data sourced from mock scrapers - Configure real scrapers for production use</p>
    </div>
    """, unsafe_allow_html=True)


# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    """Main application entry point."""
    # Initialize session state
    init_session_state()
    
    # Render header
    render_header()
    
    # Render sidebar and get config
    config = render_sidebar()
    
    # Render main content
    render_data_table(config)
    
    # Render statistics
    render_stats_section()
    
    # Render footer
    render_footer()


if __name__ == "__main__":
    main()
