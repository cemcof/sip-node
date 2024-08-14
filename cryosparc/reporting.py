import os
import datetime
import numpy as np
import bson
import csv
import matplotlib.pyplot as plt
import matplotlib.dates as dates
from matplotlib.ticker import MaxNLocator
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image
from reportlab.platypus.tables import Table, TableStyle
from pathlib import Path
import csv
import numpy as np
import pathlib
from typing import Union

class CryosparcReport:
    csv_data_keys = ['name', 'creation_time', 'total_motion', 'early_motion',
                 'late_motion','defocus','astigmatism','astigmatism_angle',
                 'resolution_CTF','relative_ice_thickness','picked_particles']
    
    def __init__(self, 
                 cs_project_path: Union[str, pathlib.Path], # '/storage/brno14-ceitec/shared/cemcof/internal/DATA_24/240729_70_rimM_KO_tilt_0270C74E/cryosparc_240729_70_rimM_KO_tilt_0270C74E',
                 bsonFile='S1/exposures.bson',
                 output_pdf_file='spa_report.pdf',
                 project_report_file='.project_report.dat',
                 csv_data_file='report_data.csv',
                 movies_local_dir='S1/import_movies',
                 motion_correction_trajectories_dir='S1/motioncorrected'):
        self.cs_project_path = Path(cs_project_path)
        self.bsonFile = bsonFile
        self.output_pdf_file = output_pdf_file
        self.project_report_file = project_report_file
        self.csv_data_file = csv_data_file
        self.movies_local_dir = movies_local_dir
        self.motion_correction_trajectories_dir = motion_correction_trajectories_dir


    def create_report(self):
        # Check if the CSV file exists, and if not, create it with headers
        csv_file_path = self.cs_project_path / self.csv_data_file
        if not csv_file_path.exists():
            with open(csv_file_path, mode='w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=self.csv_data_keys)
                writer.writeheader()

        # Check if the project report file exists and read the last processed file count
        project_report_path = self.cs_project_path / self.project_report_file
        try:
            if project_report_path.is_file():
                with open(project_report_path, 'r') as prf:
                    ls = prf.readlines()[-1].split()
                    processed_files = int(ls[0])
            else:
                processed_files = 0
        except Exception:
            processed_files = 0

        # List all files in the movies directory, sort by modification time, and filter out processed files
        movies_dir_path = self.cs_project_path / self.movies_local_dir
        files = sorted(movies_dir_path.iterdir(), key=lambda f: f.stat().st_mtime)[processed_files:]
        to_do_files = [f for f in files]

        # Read bson file and do initial sorting
        bson_file_path = self.cs_project_path / self.bsonFile
        with open(bson_file_path, 'rb') as file:
            bson_data = bson.decode_all(file.read())

        bson_id_according_to_motion = {}
        for b in range(len(bson_data[0]['exposures'])):
            bson_id_according_to_motion[bson_data[0]['exposures'][b]['groups']['exposure']['rigid_motion']['path'][0]] = b

        movie_info = {key: [] for key in self.csv_data_keys}

        # Process each movie file
        for movie in to_do_files:
            name = movie.name
            moviePref = '.'.join(name.split('.')[:-1])
            traj_file_path = self.cs_project_path / self.motion_correction_trajectories_dir / f'{moviePref}_traj.npy'
            
            if traj_file_path.is_file():
                movie_info['name'].append(name)
                movie_info['creation_time'].append(movie.stat().st_mtime)
                drift_data = np.load(traj_file_path)
                motion_vectors = [((drift_data[0][i][0] - drift_data[0][i + 1][0]) ** 2 +
                                   (drift_data[0][i][1] - drift_data[0][i + 1][1]) ** 2) ** 0.5 for i in range(drift_data.shape[1] - 1)]
                movie_info['total_motion'].append(np.sum(motion_vectors))
                movie_info['early_motion'].append(np.sum(motion_vectors[0:4]))
                movie_info['late_motion'].append(np.sum(motion_vectors[4:]))
                cur_bson_dict = bson_data[0]['exposures'][bson_id_according_to_motion[str(traj_file_path.relative_to(self.cs_project_path))]]['groups']
                movie_info['defocus'].append(min(cur_bson_dict['exposure']['ctf']['df1_A'][0], cur_bson_dict['exposure']['ctf']['df2_A'][0]))
                movie_info['astigmatism'].append(abs(cur_bson_dict['exposure']['ctf']['df1_A'][0] - cur_bson_dict['exposure']['ctf']['df2_A'][0]))
                movie_info['astigmatism_angle'].append(180. * cur_bson_dict['exposure']['ctf']['df_angle_rad'][0] / np.pi)
                movie_info['resolution_CTF'].append(cur_bson_dict['exposure']['ctf']['ctf_fit_to_A'][0])
                movie_info['relative_ice_thickness'].append(cur_bson_dict['exposure']['ctf_stats']['ice_thickness_rel'][0])
                movie_info['picked_particles'].append(cur_bson_dict['particle_blob']['count'])

        # Update the project report file with the count of processed files
        with open(project_report_path, 'a') as oF:
            oF.write('%d' % int(len(movie_info['name']) + processed_files))

        # Open the existing CSV file in append mode and write the new data
        with open(csv_file_path, mode='a', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=movie_info.keys())
            for i in range(len(next(iter(movie_info.values())))):  # Iterate based on the length of any column
                row = {key: movie_info[key][i] for key in movie_info}
                writer.writerow(row)

        data4fig_dict = {}

        # Read the CSV file and populate data for figures
        with open(csv_file_path, mode='r') as file:
            reader = csv.DictReader(file)
            for field in reader.fieldnames:
                data4fig_dict[field] = []

            for row in reader:
                for key in reader.fieldnames:
                    data4fig_dict[key].append(row[key])

        fig_list = []
        fig_list.append(self.astigPlot(data4fig_dict['astigmatism'], data4fig_dict['astigmatism_angle']))
        fig_list.append(self.histPlot('astigmatism', data4fig_dict['astigmatism']))

        for i in self.csv_data_keys:
            if i in ['name', 'creation_time', 'astigmatism', 'astigmatism_angle']:
                continue
            fig_list.append(self.timePlot(i, data4fig_dict['creation_time'], data4fig_dict[i]))
            fig_list.append(self.histPlot(i, data4fig_dict[i]))

        output_pdf_path = self.cs_project_path / self.output_pdf_file
        self.create_pdf_report(output_pdf_path, len(data4fig_dict['name']), fig_list)
        return output_pdf_path

    @staticmethod
    def create_pdf_report(output_filename, processed_imgs, plot_images):
        doc = SimpleDocTemplate(output_filename, pagesize=letter)
        styles = getSampleStyleSheet()
        story = []

        # Add introductory text
        story.append(Paragraph('SPA data collection report', styles['BodyText']))
        story.append(Spacer(1, 12))  # Add space after the text
        story.append(Paragraph('Last update: %s' % datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), styles['BodyText']))
        story.append(Paragraph('Movies processed: %d' % processed_imgs, styles['BodyText']))
        story.append(Spacer(1, 12))  # Add space after the text

        # Organize plots in two columns
        data = []
        for i in range(0, len(plot_images), 2):
            row = []
            for j in range(2):  # Two columns
                if i + j < len(plot_images):
                    img = Image(plot_images[i + j], 3 * inch, 2 * inch)
                    row.append(img)
                else:
                    row.append('')  # Add an empty cell if no plot
            data.append(row)

        # Create a table for the plots
        table = Table(data, colWidths=3.5 * inch)
        table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))

        # Add the table to the story
        story.append(table)

        # Build the PDF document
        doc.build(story)

    @staticmethod
    def timePlot(title,time,vals):
        filename = '%s_time.png' % title
        xVal = [datetime.datetime.fromtimestamp(float(i)).strftime('%H:%M:%S') for i in time]
        vals = [float(i) for i in vals]
        if len(xVal) > 1500:
            xVal = xVal[-1500:]
            vals = vals[-1500:]

        plt.gca().xaxis.set_major_formatter(dates.DateFormatter('%H:%M'))
        plt.gca().xaxis.set_major_locator(dates.HourLocator())
        plt.gca().xaxis.set_major_locator(MaxNLocator(integer=True, nbins=10))
        plt.plot(xVal, vals)
        plt.gcf().autofmt_xdate()
        plt.xlabel('Time')
        plt.ylabel('%s' % title)
        plt.savefig(filename,format='png')
        plt.close()

        return filename

    @staticmethod
    def histPlot(title,vals):
        filename = '%s_hist.png' % title
        vals = [float(i) for i in vals]
        bins = 20
        if len(vals) > 1500:
            mini = np.min(vals)
            maxi = np.max(vals)
            allDataHist = np.histogram(vals, bins=bins, range=(mini, maxi))
            latestDataHist = np.histogram(vals[len(vals) - 750:], bins=bins, range=(mini, maxi))
            latestRescaled = latestDataHist[0] * np.max(allDataHist[0]) / np.max(latestDataHist[0])
            xVal = [(allDataHist[1][i] + allDataHist[1][i - 1]) / 2. for i in range(1, len(allDataHist[1]))]
            plt.bar(xVal, allDataHist[0], width=0.4 * (xVal[-1] - xVal[0]) / bins, align='edge', label='all data')
            plt.bar(xVal + 0.5 * (xVal[1] - xVal[0]), latestRescaled, width=0.4 * (xVal[-1] - xVal[0]) / bins,
                    align='center', label='last 1500 images')
            plt.legend()
        else:
            allDataHist = np.histogram(vals, bins=bins)
            xVal = [(allDataHist[1][i] + allDataHist[1][i - 1]) / 2. for i in range(1, len(allDataHist[1]))]
            plt.bar(xVal, allDataHist[0], width=0.75 * (xVal[-1] - xVal[0]) / bins, align='center')

        plt.xlabel('%s' % title)
        plt.ylabel('Counts')
        plt.savefig(filename,format='png')
        plt.close()

        return filename

    @staticmethod
    def astigPlot(astig, astigAngle):
        filename = 'astig_plot.png'
        a = [float(i) for i in astig]
        aa = [float(i) for i in astigAngle]
        thr = 1000.0

        colors = [z for z in range(0, len(aa))]
        maxi = thr
        outOfRange = len([1 for i in a if i > thr])

        fig = plt.figure()
        ax = fig.add_axes([0.0, -0.49, 1.0, 1.0], polar=True)
        plt.scatter(aa, a, c=colors)
        plt.title('Astigmatism vs. Astigmatism angle [ ' + str(outOfRange) + ' points outside depicted area]')
        ax.set_rmax(maxi)
        plt.savefig(filename,format='png')
        plt.close()

        return filename



# try:
#     os.stat(os.path.join(cs_project_path,csv_data_file))
# except:
#     with open(os.path.join(cs_project_path,csv_data_file), mode='w', newline='') as file:
#         writer = csv.DictWriter(file, fieldnames=csv_data_keys)
#         # Write the header (column names)
#         writer.writeheader()

# try:
#     os.path.isfile(os.path.join(cs_project_path,project_report_file))
#     with open(os.path.join(cs_project_path,project_report_file),'r') as prf:
#         ls = prf.readlines()[-1].split()
#         processed_files = int(ls[0])
# except:
#     processed_files = 0

# # List all files in the directory
# files = os.listdir(os.path.join(cs_project_path,movies_local_dir))[processed_files:]
# files_full_path = [os.path.join(cs_project_path,movies_local_dir,f) for f in files]
# sorted_files = sorted(files_full_path, key=os.path.getmtime)
# to_do_files = sorted_files[processed_files:]

# # read bson file and do initial sorting
# with open(os.path.join(cs_project_path,bsonFile), 'rb') as file:
#     bson_data = bson.decode_all(file.read())

# bson_id_according_to_motion = {}
# for b in range(len(bson_data[0]['exposures'])):
#     bson_id_according_to_motion[bson_data[0]['exposures'][b]['groups']['exposure']['rigid_motion']['path'][0]] = b

# movie_info = {key:[] for key in csv_data_keys}

# for movie in to_do_files:
#     name = os.path.split(movie)[1]
#     moviePref = '.'.join(name.split('.')[:-1])
#     if os.path.isfile(os.path.join(cs_project_path,motion_correction_trajectories_dir,moviePref+'_traj.npy')):
#         movie_info['name'].append(name)
#         #movie_info['creation_time'].append(datetime.datetime.fromtimestamp(os.path.getmtime(movie)).strftime('%Y-%m-%d %H:%M:%S'))
#         movie_info['creation_time'].append(os.path.getmtime(movie))
#         drift_data_file_local_path = os.path.join(motion_correction_trajectories_dir,moviePref+'_traj.npy')
#         drift_data_file = os.path.join(cs_project_path,drift_data_file_local_path)
#         drift_data = np.load(drift_data_file)
#         motion_vectors = [((drift_data[0][i][0]-drift_data[0][i+1][0])**2+(drift_data[0][i][1]-drift_data[0][i+1][1])**2)**0.5 for i in range(drift_data.shape[1]-1)]
#         movie_info['total_motion'].append(np.sum(motion_vectors))
#         movie_info['early_motion'].append(np.sum(motion_vectors[0:4]))
#         movie_info['late_motion'].append(np.sum(motion_vectors[4:]))
#         cur_bson_dict = bson_data[0]['exposures'][bson_id_according_to_motion[drift_data_file_local_path]]['groups']
#         movie_info['defocus'].append(min(cur_bson_dict['exposure']['ctf']['df1_A'][0],cur_bson_dict['exposure']['ctf']['df2_A'][0]))
#         movie_info['astigmatism'].append(np.abs(cur_bson_dict['exposure']['ctf']['df1_A'][0]-cur_bson_dict['exposure']['ctf']['df2_A'][0]))
#         movie_info['astigmatism_angle'].append(180.*cur_bson_dict['exposure']['ctf']['df_angle_rad'][0]/np.pi)
#         movie_info['resolution_CTF'].append(cur_bson_dict['exposure']['ctf']['ctf_fit_to_A'][0])
#         movie_info['relative_ice_thickness'].append(cur_bson_dict['exposure']['ctf_stats']['ice_thickness_rel'][0])
#         movie_info['picked_particles'].append(cur_bson_dict['particle_blob']['count'])

# with open(os.path.join(cs_project_path,project_report_file),'a') as oF:
#     oF.write('%d' % int(len(movie_info['name'])+processed_files))

# # Open the existing CSV file in append mode
# with open(os.path.join(cs_project_path,csv_data_file), mode='a', newline='') as file:
#     writer = csv.DictWriter(file, fieldnames=movie_info.keys())
    
#     for i in range(len(next(iter(movie_info.values())))):  # Iterate based on the length of any column
#         row = {key: movie_info[key][i] for key in movie_info}
#         writer.writerow(row)

# data4fig_dict = {}

# # Read the CSV file
# with open(os.path.join(cs_project_path,csv_data_file), mode='r') as file:
#     reader = csv.DictReader(file)

#     # Initialize keys in the dictionary based on the header
#     for field in reader.fieldnames:
#         data4fig_dict[field] = []

#     # Populate the dictionary with data from each row
#     for row in reader:
#         for key in reader.fieldnames:
#             data4fig_dict[key].append(row[key])

# fig_list = []
# fig_list.append(astigPlot(data4fig_dict['astigmatism'],data4fig_dict['astigmatism_angle']))
# fig_list.append(histPlot('astigmatism',data4fig_dict['astigmatism']))


# for i in csv_data_keys:
#     if i in ['name','creation_time','astigmatism','astigmatism_angle']:
#         pass
#     else:
#         fig_list.append(timePlot(i,data4fig_dict['creation_time'],data4fig_dict[i]))
#         fig_list.append(histPlot(i,data4fig_dict[i]))

# create_pdf_report(os.path.join(cs_project_path,output_pdf_file),len(data4fig_dict['name']),fig_list)








