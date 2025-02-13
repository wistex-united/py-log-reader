import pandas as pd
import numpy as np
from scipy.interpolate import interp1d

def interpolate_robot_data(cog_file_path, motion_file_path, output_file_path):
    # Read the CSV files
    cog_data = pd.read_csv(cog_file_path)
    motion_data = pd.read_csv(motion_file_path)
    
    # Create interpolation functions for each speed component
    # Using 'linear' interpolation, but you can change to 'cubic' if needed
    speed_x_interp = interp1d(motion_data['timestamp'], 
                             motion_data['agentSpeedX'],
                             bounds_error=False,    # Return nan for out-of-bounds
                             fill_value='extrapolate')  # Extrapolate if needed
    
    speed_y_interp = interp1d(motion_data['timestamp'],
                             motion_data['agentSpeedY'],
                             bounds_error=False,
                             fill_value='extrapolate')
    
    speed_rot_interp = interp1d(motion_data['timestamp'],
                               motion_data['agentSpeedRot'],
                               bounds_error=False,
                               fill_value='extrapolate')
    
    # Interpolate speeds for each timestamp in cog_data
    cog_data['interpolatedSpeedX'] = speed_x_interp(cog_data['timestamp'])
    cog_data['interpolatedSpeedY'] = speed_y_interp(cog_data['timestamp'])
    cog_data['interpolatedSpeedRot'] = speed_rot_interp(cog_data['timestamp'])
    
    # Save the result to a new CSV file
    cog_data.to_csv(output_file_path, index=False)
    
    return cog_data

# Usage example
if __name__ == "__main__":
    cog_file = "cog_data.csv"
    motion_file = "motion_data.csv"
    output_file = "combined_data.csv"
    
    try:
        result_df = interpolate_robot_data(cog_file, motion_file, output_file)
        print(f"Successfully processed data. Output saved to {output_file}")
        print(f"Number of rows processed: {len(result_df)}")
        
        # Print some statistics to verify the interpolation
        print("\nInterpolated Speed Statistics:")
        print(result_df[['interpolatedSpeedX', 'interpolatedSpeedY', 'interpolatedSpeedRot']].describe())
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
