import customtkinter as ctk
import requests
from tkinter import ttk

ctk.set_appearance_mode("dark")

class AtomicPulseGUI(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("AtomicPulse | Nuclear Outage Control Center")
        self.geometry("1100x750")

        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)
        #sidebar
        self.sidebar = ctk.CTkFrame(self, width=220, corner_radius=0, fg_color="#121212")
        self.sidebar.grid(row=0, column=0, sticky="nsew")
        
        self.logo = ctk.CTkLabel(self.sidebar, text="ATOMIC\nPULSE", 
                                 font=ctk.CTkFont(size=26, weight="bold"))
        self.logo.pack(pady=(40, 60))

        self.btn_load = ctk.CTkButton(self.sidebar, text="LOAD DATA", 
                                      fg_color="#2c2c2c", hover_color="#3d3d3d",
                                      command=self.fetch_all_data)
        self.btn_load.pack(pady=10, padx=20)

        self.btn_sync = ctk.CTkButton(self.sidebar, text="SYNC WITH EIA", 
                                      border_width=1, border_color="#444",
                                      fg_color="transparent", hover_color="#1f1f1f",
                                      command=self.sync_pipeline)
        self.btn_sync.pack(pady=10, padx=20)

        self.main_content = ctk.CTkFrame(self, fg_color="transparent")
        self.main_content.grid(row=0, column=1, padx=30, pady=30, sticky="nsew")

        self.card_frame = ctk.CTkFrame(self.main_content, fg_color="transparent")
        self.card_frame.pack(fill="x", pady=(0, 20))

        self.card_total = self.create_card(self.card_frame, "TOTAL RECORDS", "0")
        self.card_avg = self.create_card(self.card_frame, "AVG OUTAGE (MW)", "0.0")
        self.card_max = self.create_card(self.card_frame, "MAX OUTAGE (MW)", "0")

        self.setup_table()

        self.status_bar = ctk.CTkLabel(self.main_content, text="System Online | Waiting for input...", 
                                        text_color="#555", font=("Arial", 12))
        self.status_bar.pack(side="bottom", anchor="w", pady=(10, 0))

    def create_card(self, parent, title, value):
        """Helper to create metric cards"""
        card = ctk.CTkFrame(parent, fg_color="#1e1e1e", height=100, corner_radius=10)
        card.pack(side="left", padx=10, expand=True, fill="both")
        
        title_lbl = ctk.CTkLabel(card, text=title, font=("Arial", 11, "bold"), text_color="#888")
        title_lbl.pack(pady=(15, 0))
        
        val_lbl = ctk.CTkLabel(card, text=value, font=("Arial", 28, "bold"), text_color="silver")
        val_lbl.pack(pady=(5, 15))
        return val_lbl

    def setup_table(self):
        """Configure the Dark Mode Treeview"""
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Treeview", background="#242424", foreground="#d1d1d1", 
                        fieldbackground="#242424", rowheight=30, borderwidth=0)
        style.configure("Treeview.Heading", background="#333", foreground="white", relief="flat")
        style.map("Treeview", background=[('selected', '#444')])

        self.tree = ttk.Treeview(self.main_content, columns=("Date", "Status", "Outage", "Percent"), show='headings')
        self.tree.heading("Date", text="DATE")
        self.tree.heading("Status", text="STATUS ID")
        self.tree.heading("Outage", text="OUTAGE (MW)")
        self.tree.heading("Percent", text="IMPACT %")
        self.tree.pack(expand=True, fill="both")

    def fetch_all_data(self):
        """Fetch metrics from /summary and rows from /data"""
        try:
            summary_res = requests.get("http://127.0.0.1:8000/summary")
            if summary_res.status_code == 200:
                s = summary_res.json()
                self.card_total.configure(text=str(s["total_records"]))
                self.card_avg.configure(text=f"{s['avg_outage_mw']:.2f}")
                self.card_max.configure(text=f"{s['max_outage_mw']:.1f}")

            data_res = requests.get("http://127.0.0.1:8000/data?limit=50")
            if data_res.status_code == 200:
                rows = data_res.json()
                for i in self.tree.get_children(): self.tree.delete(i)
                if not rows:
                    self.status_bar.configure(text="No data found in database. Please click 'SYNC WITH EIA'.", text_color = "#FFB86C")
                    return 
                for row in rows:
                    self.tree.insert("", "end", values=(row["date_key"], row["status_id"], row["outage_mw"], f"{row['percent_outage']}%"))
                
                self.status_bar.configure(text="Data successfully loaded from local database.", text_color="#4CAF50")

            elif data_res.status_code == 500:
                self.status_bar.configure(text="Database not initialized. Please click 'SYNC WITH EIA'.", text_color="#FFB86C")
        except:
            self.status_bar.configure(text="Connection Error: Is the FastAPI server running?", text_color="#F44336")

    def sync_pipeline(self):
        """Trigger the /refresh endpoint via POST"""
        try:
            self.status_bar.configure(text="Syncing with EIA API... please wait.", text_color="#00BCD4")
            self.update() 
            
            response = requests.post("http://127.0.0.1:8000/refresh")
            
            if response.status_code == 200:
                res = response.json()
                msg = f"✅ {res['message']} ({res['records_processed']} new records)"
                self.status_bar.configure(text=msg, text_color="#4CAF50")
                self.fetch_all_data()
            else:
                self.status_bar.configure(text="Sync failed at API level.", text_color="#F44336")
        except:
            self.status_bar.configure(text="Fatal Error: API unreachable.", text_color="#F44336")

if __name__ == "__main__":
    app = AtomicPulseGUI()
    app.mainloop()
