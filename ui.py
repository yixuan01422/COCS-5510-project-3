import tkinter as tk
from tkinter import scrolledtext
from dbms.core import SimpleDBMS

class DBMSApp:
    def __init__(self, root):
        self.db = SimpleDBMS()
        root.title("SimpleDBMS Interface")
        root.configure(bg="#1e1e1e")

        # SQL Input Label
        label = tk.Label(root, text="Enter SQL Query:", fg="white", bg="#1e1e1e", font=("Arial", 12, "bold"))
        label.pack(pady=(15, 0))

        # SQL Input Text Field
        self.input_box = tk.Text(
            root, height=5, width=80, bg="#1e1e1e", fg="white",
            insertbackground="white", font=("Courier", 12)
        )
        self.input_box.pack(pady=(5, 10))

        # Execute Button
        self.execute_button = tk.Button(
            root, text="Execute", command=self.execute_query,
            bg="#3a3a3a", fg="white", font=("Arial", 12, "bold")
        )
        self.execute_button.pack(pady=(5, 10))

        # Output Area
        self.output_area = scrolledtext.ScrolledText(
            root, height=15, width=100, bg="#121212", fg="white",
            font=("Courier", 11), insertbackground="white"
        )
        self.output_area.pack(pady=(10, 20))
        self.output_area.config(state=tk.DISABLED)

    def execute_query(self):
        query = self.input_box.get("1.0", tk.END).strip()
        self.output_area.config(state=tk.NORMAL)
        self.output_area.delete("1.0", tk.END)

        if query:
            try:
                result = self.db.execute(query)
                if isinstance(result, list):
                    for row in result:
                        self.output_area.insert(tk.END, str(row) + "\n")
                elif result is not None:
                    self.output_area.insert(tk.END, str(result) + "\n")
                else:
                    self.output_area.insert(tk.END, "Query executed.\n")
            except Exception as e:
                self.output_area.insert(tk.END, f"Error: {str(e)}\n")
        else:
            self.output_area.insert(tk.END, "Please enter a SQL command.\n")

        self.output_area.config(state=tk.DISABLED)

if __name__ == "__main__":
    root = tk.Tk()
    app = DBMSApp(root)
    root.mainloop()
